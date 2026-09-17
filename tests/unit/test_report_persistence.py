"""Unit tests for Validation Report Persistence (Phase M5L).

Covers requirements:
1. Online validation automatically creates validation.json.
2. Online validation automatically creates validation.md.
3. JSON contains the complete validation result.
4. Markdown contains the major validation sections.
5. Timestamped runs do not overwrite previous reports.
6. Custom --output-dir works.
7. Credentials are never persisted.
8. Existing CLI output remains intact.
9. Offline validation persists JSON.
10. Offline validation persists Markdown.
11. Existing offline output behavior remains intact.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from dpif.cli import cli
from dpif.connectors.base import DatabricksConnector
from dpif.orchestration.online import OnlineValidationOrchestrator
from dpif.reporting.validation_reports import (
    format_filesystem_timestamp,
    get_report_paths,
    persist_online_validation_report,
    sanitize_dict_credentials,
)


class MockDiscoveryConnector(DatabricksConnector):
    """Configurable mock connector for persistence tests."""

    def __init__(
        self,
        job_payload: dict[str, Any] | None = None,
    ) -> None:
        self.job_payload = job_payload or {}
        self.cluster_payload = {
            "cluster_id": "c-123",
            "cluster_name": "test_cluster",
            "spark_version": "14.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 4,
        }
        self.runtime_payload = {
            "run_id": 9999,
            "execution_duration": 120000,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        }
        self.recent_runs_payload = {"runs": [{"run_id": 9999}]}

    def mode(self) -> str:
        return "live-api"

    def get_workspace_status(self) -> dict[str, Any] | None:
        return {"status": "connected", "spark_versions_count": 3}

    def get_job(self, job_id: int | str) -> dict[str, Any] | None:
        return self.job_payload

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        return self.cluster_payload

    def get_recent_runs(
        self, job_id: int | str, limit: int = 10
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        return self.recent_runs_payload

    def get_run(self, run_id: int | str) -> dict[str, Any] | None:
        return self.runtime_payload

    def get_table_profile(self, table: str) -> dict[str, Any] | None:
        return None


def test_timestamp_format():
    ts = format_filesystem_timestamp()
    assert ":" not in ts
    assert len(ts) == 19
    assert ts[10] == "T"


def test_get_report_paths_creation(tmp_path: Path):
    json_path, md_path = get_report_paths(
        base_dir=tmp_path,
        mode="online",
        resource_id="job_12345",
        timestamp_str="2026-09-17T12-00-00",
    )
    assert json_path.name == "validation.json"
    assert md_path.name == "validation.md"
    assert json_path.parent == tmp_path / "online" / "job_12345" / "2026-09-17T12-00-00"
    assert json_path.parent.exists()


def test_timestamped_runs_do_not_overwrite(tmp_path: Path):
    json_p1, md_p1 = get_report_paths(
        base_dir=tmp_path,
        mode="online",
        resource_id="res-1",
        timestamp_str="2026-09-17T12-00-00",
    )
    json_p1.write_text("run1", encoding="utf-8")

    json_p2, md_p2 = get_report_paths(
        base_dir=tmp_path,
        mode="online",
        resource_id="res-1",
        timestamp_str="2026-09-17T12-00-05",
    )
    json_p2.write_text("run2", encoding="utf-8")

    assert json_p1 != json_p2
    assert json_p1.read_text(encoding="utf-8") == "run1"
    assert json_p2.read_text(encoding="utf-8") == "run2"


def test_sanitize_dict_credentials_masks_tokens():
    raw = {
        "token": "dapi_secret_token_12345",
        "url": "https://adb-123.net/?token=dapi_secret_token_12345",
        "nested": {"auth": "Bearer dapi_secret_token_12345"},
    }
    cleaned = sanitize_dict_credentials(raw)
    dumped = json.dumps(cleaned)
    assert "dapi_secret_token_12345" not in dumped
    assert "[MASKED" in dumped


def test_persist_online_validation_report_full_fidelity(tmp_path: Path):
    conn = MockDiscoveryConnector(
        job_payload={
            "job_id": 999,
            "settings": {"name": "test_job", "tasks": []},
        }
    )
    orchestrator = OnlineValidationOrchestrator(connector=conn)
    result = orchestrator.validate(job_id=999)

    json_p, md_p = persist_online_validation_report(result, output_dir=tmp_path)
    assert json_p.exists()
    assert md_p.exists()

    with open(json_p, encoding="utf-8") as f:
        data = json.load(f)

    # Verify JSON content
    assert data["execution_mode"] == "online"
    assert data["resource"]["id"] == "999"
    assert "timestamp" in data
    assert "checkpoints" in data
    assert "CP-001" in data["checkpoints"]
    assert "implementation_forensics" in data
    assert "rerun_analysis" in data
    assert "alignment_analysis" in data
    assert "evidence_sufficiency" in data
    assert "decision_risk_synthesis" in data
    assert "quality_score" in data
    assert "final_decision" in data

    # Verify Markdown content
    md_text = md_p.read_text(encoding="utf-8")
    assert "# DPIF Validation Report (Online)" in md_text
    assert "## Validation Summary" in md_text
    assert "## Evidence Summary" in md_text
    assert "## Checkpoint Results (CP-001..CP-024)" in md_text
    assert "## Developer Implementation Forensics (M5E)" in md_text
    assert "## Rerun / Idempotency Forensics (M5F)" in md_text
    assert "## Three-Layer Alignment Forensics (M5G)" in md_text
    assert "## Evidence Sufficiency (M5H)" in md_text
    assert "## Production Decision (CP-FINAL & M5I)" in md_text


def test_cli_validate_online_creates_files(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://mock.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token-xyz")

    from dpif.connectors.live import LiveDatabricksConnector

    conn = MockDiscoveryConnector(
        job_payload={"job_id": 8888, "settings": {"name": "cli_job", "tasks": []}}
    )
    monkeypatch.setattr(LiveDatabricksConnector, "get_job", lambda self, jid: conn.get_job(jid))
    monkeypatch.setattr(LiveDatabricksConnector, "get_cluster", lambda self, cid: conn.get_cluster(cid))
    monkeypatch.setattr(LiveDatabricksConnector, "get_recent_runs", lambda self, jid, limit=10: conn.get_recent_runs(jid, limit))

    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "validate-online",
            "--job-id",
            "8888",
            "--output-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 0
    assert "DPIF ONLINE VALIDATION" in res.output
    assert "VALIDATION REPORTS PERSISTED" in res.output

    # Verify report directory
    target_dir = tmp_path / "online" / "8888"
    assert target_dir.exists()
    runs = list(target_dir.iterdir())
    assert len(runs) == 1
    run_dir = runs[0]
    assert (run_dir / "validation.json").exists()
    assert (run_dir / "validation.md").exists()

    # Verify no credentials leaked
    assert "mock-token-xyz" not in (run_dir / "validation.json").read_text(encoding="utf-8")
    assert "mock-token-xyz" not in (run_dir / "validation.md").read_text(encoding="utf-8")


def test_cli_validate_online_json_flag_preserves_pure_json_and_persists_files(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://mock.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "mock-token-xyz")

    from dpif.connectors.live import LiveDatabricksConnector

    conn = MockDiscoveryConnector(
        job_payload={"job_id": 7777, "settings": {"name": "json_job", "tasks": []}}
    )
    monkeypatch.setattr(LiveDatabricksConnector, "get_job", lambda self, jid: conn.get_job(jid))
    monkeypatch.setattr(LiveDatabricksConnector, "get_cluster", lambda self, cid: conn.get_cluster(cid))
    monkeypatch.setattr(LiveDatabricksConnector, "get_recent_runs", lambda self, jid, limit=10: conn.get_recent_runs(jid, limit))

    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "validate-online",
            "--job-id",
            "7777",
            "--output-dir",
            str(tmp_path),
            "--json",
        ],
    )
    assert res.exit_code == 0
    parsed = json.loads(res.output)
    assert parsed["resource"]["id"] == "7777"

    # Files must also be saved to disk
    target_dir = tmp_path / "online" / "7777"
    assert target_dir.exists()
    runs = list(target_dir.iterdir())
    assert len(runs) == 1
    assert (runs[0] / "validation.json").exists()
    assert (runs[0] / "validation.md").exists()


def test_cli_validate_offline_creates_files(tmp_path: Path):
    contract_path = Path("examples/customer_daily.yaml")
    assert contract_path.exists()

    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(contract_path),
            "--offline",
            "--output-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 0
    assert "VALIDATION REPORTS PERSISTED" in res.output

    # Target directory under offline
    target_dir = tmp_path / "offline" / "customer_daily"
    assert target_dir.exists()
    runs = list(target_dir.iterdir())
    assert len(runs) == 1
    run_dir = runs[0]
    json_file = run_dir / "validation.json"
    md_file = run_dir / "validation.md"
    assert json_file.exists()
    assert md_file.exists()

    # Content verification
    data = json.loads(json_file.read_text(encoding="utf-8"))
    assert data["execution_mode"] == "offline"
    assert data["pipeline_name"] == "customer_daily"
    assert "customer_daily" in data["contract_id"]
    assert "checkpoints" in data
    assert "decision_risk_synthesis" in data

    md_text = md_file.read_text(encoding="utf-8")
    assert "# DPIF Validation Report (Offline)" in md_text
    assert "## Validation Summary" in md_text
    assert "customer_daily" in md_text
