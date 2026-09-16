"""Unit tests for OnlineValidationOrchestrator (M5J).

Tests orchestrator initialization, input validation, auto-discovery of cluster/run,
historical run preservation semantics, credential masking, and CLI commands.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from click.testing import CliRunner

from dpif.cli import cli
from dpif.connectors.base import DatabricksConnector
from dpif.orchestration.online import (
    OnlineValidationOrchestrator,
    OnlineValidationResult,
    run_online_validation,
)
from dpif.providers.base import EvidenceCategory


class DummyConnector(DatabricksConnector):
    def __init__(self, host: str = "https://dummy.databricks.com", token: str = "dapi_secret_123") -> None:
        self.host = host
        self._token = token

    def mode(self) -> str:
        return "live-api"

    def get_workspace_status(self) -> dict[str, Any] | None:
        return {"host": self.host, "status": "connected", "spark_versions_count": 5}

    def get_job(self, job_id: int | str) -> dict[str, Any] | None:
        return {
            "job_id": int(job_id) if str(job_id).isdigit() else job_id,
            "settings": {
                "name": "auto_discovered_job",
                "max_retries": 1,
                "tasks": [{"task_key": "main", "existing_cluster_id": "cluster-auto-123"}],
            },
            "secret_key": "dapi_secret_123",
        }

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        return {
            "cluster_id": cluster_id,
            "cluster_name": "auto_cluster",
            "num_workers": 2,
            "node_type_id": "Standard_D8s_v5",
            "spark_version": "14.3.x-scala2.12",
        }

    def get_cluster_policy(self, policy_id: str) -> dict[str, Any] | None:
        return None

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        return {"pipeline_id": pipeline_id, "name": f"dlt_{pipeline_id}"}

    def get_permissions(self, object_type: str, object_id: str) -> dict[str, Any] | None:
        return {"access_control_list": []}

    def get_table_profile(self, table: str) -> dict[str, Any]:
        return {"table": table, "status": "runtime-scan-required"}

    def get_recent_runs(
        self, job_id: int | str, limit: int = 10
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        return {"runs": [{"run_id": 8881, "execution_duration": 60000}]}

    def get_run(self, run_id: int | str) -> dict[str, Any] | None:
        return {
            "run_id": int(run_id) if str(run_id).isdigit() else run_id,
            "execution_duration": 60000,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        }


def test_orchestrator_initialization_with_env(monkeypatch: pytest.MonkeyPatch):
    """Test orchestrator correctly resolves host and token from environment variables."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://env-workspace.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_env_token_456")

    orchestrator = OnlineValidationOrchestrator()
    assert orchestrator.workspace == "https://env-workspace.databricks.com"
    assert orchestrator.token == "dapi_env_token_456"


def test_orchestrator_missing_job_and_pipeline_raises_value_error():
    """Test ValueError is raised when neither job_id nor pipeline_id is provided."""
    conn = DummyConnector()
    orchestrator = OnlineValidationOrchestrator(connector=conn)
    with pytest.raises(ValueError, match="Either job_id or pipeline_id must be provided"):
        orchestrator.validate()


def test_orchestrator_auto_discovers_cluster_from_job_tasks():
    """Test that cluster is auto-discovered from job task when not explicitly given."""
    conn = DummyConnector()
    orchestrator = OnlineValidationOrchestrator(connector=conn)
    res = orchestrator.validate(job_id=456)

    assert res.resource_type == "job"
    assert res.resource_id == "456"
    assert res.pipeline_name == "auto_discovered_job"
    assert res.evidence_summary["cluster"] == "LIVE"
    assert res.checkpoints.get("CP-009") is not None
    assert res.checkpoints["CP-009"].status.value == "PASS"


def test_orchestrator_auto_discovers_latest_run_when_run_id_omitted():
    """Test that latest run is automatically discovered from recent runs list."""
    conn = DummyConnector()
    orchestrator = OnlineValidationOrchestrator(connector=conn)
    res = orchestrator.validate(job_id=456)

    assert res.evidence_summary["runtime"] == "LIVE"
    runtime_item = res.normalized_evidence.items.get(EvidenceCategory.RUNTIME.value)
    assert runtime_item is not None
    assert runtime_item.payload.get("run_id") == 8881


def test_orchestrator_pipeline_resource_type():
    """Test online validation with explicit pipeline_id."""
    conn = DummyConnector()
    orchestrator = OnlineValidationOrchestrator(connector=conn)
    res = orchestrator.validate(pipeline_id="dlt_pipeline_xyz")

    assert res.resource_type == "pipeline"
    assert res.resource_id == "dlt_pipeline_xyz"
    assert res.pipeline_name == "dlt_dlt_pipeline_xyz"
    assert res.evidence_summary["pipeline"] == "LIVE"


def test_orchestrator_convenience_function():
    """Test run_online_validation functional wrapper."""
    conn = DummyConnector()
    res = run_online_validation(job_id=456, connector=conn)
    assert isinstance(res, OnlineValidationResult)
    assert res.resource_id == "456"


def test_orchestrator_to_dict_sanitization():
    """Verify result.to_dict() has execution_mode='online', evidence_provenance, and zero leaked tokens."""
    conn = DummyConnector()
    res = run_online_validation(job_id=456, connector=conn)
    d = res.to_dict()

    assert d["execution_mode"] == "online"
    assert d["resource"]["id"] == "456"
    assert "quality_score" in d
    assert "final_decision" in d
    assert "evidence_provenance" in d
    assert d["evidence_provenance"]["cluster"] == "LIVE_API"

    s = json.dumps(d)
    assert "dapi_secret_123" not in s


def test_cli_validate_online_requires_resource():
    """Test CLI returns error code 2 when neither job-id nor pipeline-id is passed."""
    runner = CliRunner()
    res = runner.invoke(cli, ["validate-online"])
    assert res.exit_code == 2
    assert "Error: Either --job-id or --pipeline-id is required" in res.output


def test_cli_validate_flag_online_requires_resource():
    """Test dpif validate --online requires job-id or pipeline-id."""
    runner = CliRunner()
    res = runner.invoke(cli, ["validate", "--online"])
    assert res.exit_code == 2
    assert "Error: Either --job-id or --pipeline-id is required" in res.output
