"""Unit tests for Phase 6 Databricks Connectors (Offline and Live)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from dpif.connectors.live import DatabricksAPIError, LiveDatabricksConnector
from dpif.connectors.offline import OfflineDatabricksConnector
from dpif.discovery.analyzers import _to_cluster, _to_job


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


class TestOfflineDatabricksConnector:
    def test_get_job_good_fixture(self, repo_root):
        conn = OfflineDatabricksConnector(repo_root / "tests" / "fixtures" / "databricks")
        job_data = conn.get_job("job_good.json")
        assert job_data is not None
        assert job_data["_connector"] == "offline-fixture"
        assert job_data["evidence_source"] == "FIXTURE"

        job = _to_job(job_data)
        assert job is not None
        assert job.name == "customer_daily_etl"
        assert len(job.tasks) == 2
        assert job.schedule is not None
        assert job.schedule.pause_status == "UNPAUSED"

    def test_get_job_bad_fixture(self, repo_root):
        conn = OfflineDatabricksConnector(repo_root / "tests" / "fixtures" / "databricks")
        job_data = conn.get_job("job_bad.json")
        assert job_data is not None

        job = _to_job(job_data)
        assert job is not None
        assert job.tasks[0].max_retries == 0
        assert job.tasks[0].timeout_seconds == 0

    def test_get_cluster_good_fixture(self, repo_root):
        conn = OfflineDatabricksConnector(repo_root / "tests" / "fixtures" / "databricks")
        cluster_data = conn.get_cluster("cluster_good.json")
        assert cluster_data is not None
        assert cluster_data["_connector"] == "offline-fixture"

        cluster = _to_cluster(cluster_data)
        assert cluster is not None
        assert cluster.cluster_name == "prod_etl_cluster"
        assert cluster.has_autoscaling is True
        assert cluster.is_photon is True
        assert cluster.autotermination_minutes == 30

    def test_get_cluster_policy_fixture(self, repo_root):
        conn = OfflineDatabricksConnector(repo_root / "tests" / "fixtures" / "databricks")
        policy = conn.get_cluster_policy("policy_standard.json")
        assert policy is not None
        assert policy["name"] == "Standard Production Policy"
        assert "autotermination_minutes" in policy["definition"]

    def test_get_pipeline_fixture(self, repo_root):
        conn = OfflineDatabricksConnector(repo_root / "tests" / "fixtures" / "databricks")
        pipeline = conn.get_pipeline("pipeline_good.json")
        assert pipeline is not None
        assert pipeline["name"] == "customers_dlt_pipeline"
        assert pipeline["edition"] == "ADVANCED"
        assert pipeline["continuous"] is False

    def test_get_permissions_fixture(self, repo_root):
        conn = OfflineDatabricksConnector(repo_root / "tests" / "fixtures" / "databricks")
        perms = conn.get_permissions("job", "permissions_prod.json")
        assert perms is not None
        assert len(perms["access_control_list"]) == 2

    def test_missing_fixture_returns_none(self, repo_root):
        conn = OfflineDatabricksConnector(repo_root / "tests" / "fixtures" / "databricks")
        assert conn.get_cluster_policy("non_existent_policy.json") is None
        assert conn.get_pipeline("non_existent_pipeline.json") is None
        assert not conn._find_fixture("job", "non_existent_job.json")


class TestLiveDatabricksConnector:
    def test_init_cleans_host_url(self):
        conn = LiveDatabricksConnector("https://adb-123.azuredatabricks.net/", "secret-token")
        assert conn.host == "https://adb-123.azuredatabricks.net"
        assert conn.headers["Authorization"] == "Bearer secret-token"

    @patch("requests.request")
    def test_get_job_live_success(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "job_id": 100,
            "settings": {
                "name": "live_job",
                "tasks": [{"task_key": "step1", "notebook_task": {"notebook_path": "/p"}}],
            },
        }
        mock_request.return_value = mock_resp

        conn = LiveDatabricksConnector("https://adb-123.net", "token")
        job_data = conn.get_job("100")
        assert job_data is not None
        assert job_data["settings"]["name"] == "live_job"
        assert len(job_data["settings"]["tasks"]) == 1

    @patch("requests.request")
    def test_get_cluster_404_returns_none(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_request.return_value = mock_resp

        conn = LiveDatabricksConnector("https://adb-123.net", "token")
        cluster = conn.get_cluster("cluster-missing")
        assert cluster is None

    @patch("requests.request")
    def test_auth_error_401_raises_databricks_api_error_without_token_leakage(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "Invalid access token"
        mock_request.return_value = mock_resp

        secret_token = "ultra-secret-key-12345"
        conn = LiveDatabricksConnector("https://adb-123.net", secret_token)
        with pytest.raises(DatabricksAPIError) as exc_info:
            conn.get_workspace_status()
        assert exc_info.value.status_code == 401
        assert secret_token not in str(exc_info.value)
        assert secret_token not in repr(exc_info.value)

    @patch("requests.request")
    def test_rate_limit_429_raises_databricks_api_error(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.text = "Too Many Requests"
        mock_request.return_value = mock_resp

        conn = LiveDatabricksConnector("https://adb-123.net", "token")
        with pytest.raises(DatabricksAPIError) as exc_info:
            conn.get_job("100")
        assert exc_info.value.status_code == 429

    @patch("requests.request")
    def test_timeout_raises_databricks_api_error(self, mock_request):
        mock_request.side_effect = requests.Timeout("Connection timed out after 30s")

        conn = LiveDatabricksConnector("https://adb-123.net", "token")
        with pytest.raises(DatabricksAPIError) as exc_info:
            conn.get_cluster("c1")
        assert "timed out" in str(exc_info.value).lower()
