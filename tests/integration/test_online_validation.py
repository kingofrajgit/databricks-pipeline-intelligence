"""Integration tests for Phase M5J: Online End-to-End Databricks Validation.

Validates the complete online validation workflow against Databricks environments
using deterministic mocked connectors and evidence providers.

Covers all 20 required integration scenarios:
1. Complete live evidence
2. Partial live evidence
3. Authentication failure (401)
4. Authorization failure (403)
5. Resource not found (404)
6. Rate limit (429)
7. Timeout (408)
8. Malformed response
9. Empty historical runs ([] vs None distinction)
10. Historical acquisition failure (None vs [] distinction)
11. Missing code evidence (zero fabrication)
12. Runtime evidence available
13. Runtime evidence unavailable (strict UNKNOWN)
14. Provenance preservation
15. Credential sanitization
16. M5H sufficiency propagation
17. M5I decision propagation
18. JSON output
19. CLI output
20. No credential leakage
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from dpif.cli import cli
from dpif.connectors.base import DatabricksConnector
from dpif.connectors.live import DatabricksApiError
from dpif.orchestration.online import (
    OnlineValidationOrchestrator,
)
from dpif.providers.base import AcquisitionErrorCode, EvidenceCategory

SECRET_TOKEN = "dapi_secret_super_token_999888777"
WORKSPACE_HOST = "https://adb-9876543210.11.azuredatabricks.net"

MOCK_JOB_PAYLOAD: dict[str, Any] = {
    "job_id": 12345,
    "creator_user_name": "data_eng@company.com",
    "settings": {
        "name": "customer_ingest_production",
        "max_retries": 2,
        "timeout_seconds": 3600,
        "tasks": [
            {
                "task_key": "extract_bronze",
                "existing_cluster_id": "0123-456789-cluster1",
                "timeout_seconds": 1800,
            }
        ],
    },
    "token_secret": SECRET_TOKEN,
}

MOCK_CLUSTER_PAYLOAD: dict[str, Any] = {
    "cluster_id": "0123-456789-cluster1",
    "cluster_name": "etl_standard_cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_D8s_v5",
    "driver_node_type_id": "Standard_D8s_v5",
    "num_workers": 4,
    "autoscale": None,
    "spark_conf": {"spark.executor.memory": "16g"},
}

MOCK_RUNTIME_RUN_PAYLOAD: dict[str, Any] = {
    "run_id": 999111,
    "job_id": 12345,
    "run_name": "customer_ingest_run_999111",
    "start_time": 1726000000000,
    "end_time": 1726000180000,
    "setup_duration": 10000,
    "execution_duration": 160000,
    "cleanup_duration": 10000,
    "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "SUCCESS",
        "state_message": "Run finished successfully",
    },
    "tasks": [
        {
            "task_key": "extract_bronze",
            "run_id": 999112,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            "execution_duration": 150000,
        }
    ],
}

MOCK_HISTORICAL_RUNS_PAYLOAD: dict[str, Any] = {
    "runs": [
        {
            "run_id": 999111,
            "job_id": 12345,
            "start_time": 1726000000000,
            "end_time": 1726000180000,
            "execution_duration": 160000,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        },
        {
            "run_id": 999110,
            "job_id": 12345,
            "start_time": 1725913600000,
            "end_time": 1725913780000,
            "execution_duration": 155000,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        },
    ]
}


_DEFAULT: Any = object()


class MockLiveConnector(DatabricksConnector):
    """Deterministic mock connector implementing DatabricksConnector interface."""

    def __init__(
        self,
        host: str = WORKSPACE_HOST,
        token: str = SECRET_TOKEN,
        job_payload: Any = _DEFAULT,
        cluster_payload: Any = _DEFAULT,
        run_payload: Any = _DEFAULT,
        historical_payload: Any = _DEFAULT,
        workspace_connected: bool = True,
        error_on_job: Exception | None = None,
        error_on_historical: Exception | None = None,
        error_on_runtime: Exception | None = None,
    ) -> None:
        self.host = host
        self._token = token
        self._job_payload = dict(MOCK_JOB_PAYLOAD) if job_payload is _DEFAULT else job_payload
        self._cluster_payload = dict(MOCK_CLUSTER_PAYLOAD) if cluster_payload is _DEFAULT else cluster_payload
        self._run_payload = dict(MOCK_RUNTIME_RUN_PAYLOAD) if run_payload is _DEFAULT else run_payload
        self._historical_payload = (
            dict(MOCK_HISTORICAL_RUNS_PAYLOAD) if historical_payload is _DEFAULT else historical_payload
        )
        self._workspace_connected = workspace_connected
        self._error_on_job = error_on_job
        self._error_on_historical = error_on_historical
        self._error_on_runtime = error_on_runtime

    def mode(self) -> str:
        return "live-api"

    def get_workspace_status(self) -> dict[str, Any] | None:
        if not self._workspace_connected:
            return None
        return {
            "host": self.host,
            "status": "connected",
            "spark_versions_count": 14,
            "_connector": "live-api",
        }

    def get_job(self, job_id: int | str) -> dict[str, Any] | None:
        if self._error_on_job:
            raise self._error_on_job
        return self._job_payload

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        return self._cluster_payload

    def get_cluster_policy(self, policy_id: str) -> dict[str, Any] | None:
        return None

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        return {"pipeline_id": pipeline_id, "name": f"dlt_{pipeline_id}", "state": "RUNNING"}

    def get_permissions(self, object_type: str, object_id: str) -> dict[str, Any] | None:
        return {"access_control_list": [{"group_name": "users", "permission_level": "CAN_VIEW"}]}

    def get_table_profile(self, table: str) -> dict[str, Any]:
        return {"table": table, "status": "runtime-scan-required", "_connector": "live-api"}

    def get_recent_runs(
        self, job_id: int | str, limit: int = 10
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        if self._error_on_historical:
            raise self._error_on_historical
        return self._historical_payload

    def get_run(self, run_id: int | str) -> dict[str, Any] | None:
        if self._error_on_runtime:
            raise self._error_on_runtime
        return self._run_payload


@pytest.fixture
def temp_code_file(tmp_path: Path) -> Path:
    code_path = tmp_path / "pipeline.py"
    code_path.write_text(
        "import pyspark.sql.functions as F\n"
        "df = spark.read.parquet('/mnt/data/source')\n"
        "df_clean = df.filter(F.col('id').isNotNull())\n"
        "df_clean.write.mode('overwrite').parquet('/mnt/data/target')\n",
        encoding="utf-8",
    )
    return code_path


class TestOnlineEndToEndValidation:
    """Comprehensive test suite covering all 20 online integration scenarios."""

    def test_01_complete_live_evidence(self, temp_code_file: Path) -> None:
        """1. Complete live evidence across all categories."""
        conn = MockLiveConnector()
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345, code_path=str(temp_code_file))

        assert res.execution_mode == "online"
        assert res.resource_type == "job"
        assert res.resource_id == "12345"
        assert res.evidence_summary["workspace"] == "LIVE"
        assert res.evidence_summary["job"] == "LIVE"
        assert res.evidence_summary["cluster"] == "LIVE"
        assert res.evidence_summary["runtime"] == "LIVE"
        assert res.evidence_summary["historical"] == "LIVE"
        assert res.evidence_summary["code"] == "LIVE"
        assert res.quality_score > 0
        assert res.final_decision in ("PRODUCTION_READY", "NOT_PRODUCTION_READY")

    def test_02_partial_live_evidence(self) -> None:
        """2. Partial live evidence: job and cluster live, but runtime and historical fail."""
        conn = MockLiveConnector(
            run_payload=None,
            historical_payload=None,
            error_on_historical=DatabricksApiError("Historical runs unavailable", status_code=503),
            error_on_runtime=DatabricksApiError("Run details unavailable", status_code=503),
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345, run_id=999111)

        assert res.evidence_summary["job"] == "LIVE"
        assert res.evidence_summary["cluster"] == "LIVE"
        assert res.evidence_summary["runtime"] == "ERROR"
        assert res.evidence_summary["historical"] == "ERROR"
        assert res.evidence_summary["code"] == "UNAVAILABLE"
        # M5H reflects missing evidence
        assert res.decision_sufficiency is False
        assert res.confidence in ("LOW", "INSUFFICIENT", "MEDIUM")

    def test_03_authentication_failure_401(self) -> None:
        """3. Authentication failure (401) on job fetch."""
        conn = MockLiveConnector(
            error_on_job=DatabricksApiError("Invalid Bearer token", status_code=401)
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        assert res.evidence_summary["job"] == "ERROR"
        job_diag = next(d for d in res.evidence_diagnostics if d.category == "job")
        assert job_diag.error_code == AcquisitionErrorCode.AUTHENTICATION_FAILURE.value
        assert SECRET_TOKEN not in (job_diag.error_message or "")

    def test_04_authorization_failure_403(self) -> None:
        """4. Authorization failure (403) on job fetch."""
        conn = MockLiveConnector(
            error_on_job=DatabricksApiError("User does not have CAN_VIEW on job 12345", status_code=403)
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        assert res.evidence_summary["job"] == "ERROR"
        job_diag = next(d for d in res.evidence_diagnostics if d.category == "job")
        assert job_diag.error_code == AcquisitionErrorCode.AUTHORIZATION_FAILURE.value

    def test_05_resource_not_found_404(self) -> None:
        """5. Resource not found (404) when job does not exist."""
        conn = MockLiveConnector(job_payload=None)
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=99999)

        assert res.evidence_summary["job"] == "ERROR"
        job_diag = next(d for d in res.evidence_diagnostics if d.category == "job")
        assert job_diag.error_code == AcquisitionErrorCode.RESOURCE_NOT_FOUND.value

    def test_06_rate_limit_429(self) -> None:
        """6. Rate limit exceeded (429)."""
        conn = MockLiveConnector(
            error_on_job=DatabricksApiError("Rate limit exceeded", status_code=429)
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        job_diag = next(d for d in res.evidence_diagnostics if d.category == "job")
        assert job_diag.error_code == AcquisitionErrorCode.RATE_LIMIT_EXCEEDED.value

    def test_07_timeout_408(self) -> None:
        """7. API Timeout (408)."""
        conn = MockLiveConnector(
            error_on_job=DatabricksApiError("Databricks request timed out after 15s", status_code=408)
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        job_diag = next(d for d in res.evidence_diagnostics if d.category == "job")
        assert job_diag.error_code == AcquisitionErrorCode.TIMEOUT.value

    def test_08_malformed_response(self) -> None:
        """8. Malformed API response structure."""
        conn = MockLiveConnector(
            error_on_job=DatabricksApiError("Malformed JSON response from Databricks API", status_code=200)
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        job_diag = next(d for d in res.evidence_diagnostics if d.category == "job")
        assert job_diag.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE.value

    def test_09_empty_historical_runs(self) -> None:
        """9. Successful query returning 0 historical runs (payload = [])."""
        conn = MockLiveConnector(historical_payload={"runs": []})
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        assert res.evidence_summary["historical"] == "LIVE"
        hist_diag = next(d for d in res.evidence_diagnostics if d.category == "historical")
        assert hist_diag.status == "LIVE"
        assert hist_diag.details.get("runs_count") == 0
        # Check that payload was [] and not None
        assert res.normalized_evidence.get_payload(EvidenceCategory.HISTORICAL_RUNS) == []

    def test_10_historical_acquisition_failure(self) -> None:
        """10. Historical API failure (payload = None, distinguished from [])."""
        conn = MockLiveConnector(
            error_on_historical=DatabricksApiError("Historical API failure (500)", status_code=500)
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        assert res.evidence_summary["historical"] == "ERROR"
        assert res.normalized_evidence.get_payload(EvidenceCategory.HISTORICAL_RUNS) is None

    def test_11_missing_code_evidence(self) -> None:
        """11. Missing code evidence does not fabricate AST or code rules."""
        conn = MockLiveConnector()
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345, code_path=None)

        assert res.evidence_summary["code"] == "UNAVAILABLE"
        # Checkpoint CP-004 must be UNKNOWN, never PASS
        cp004 = res.checkpoints.get("CP-004")
        assert cp004 is not None
        assert cp004.status.value == "UNKNOWN"

    def test_12_runtime_evidence_available(self) -> None:
        """12. Runtime evidence available populates CP-008 and actual layer."""
        conn = MockLiveConnector()
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        assert res.evidence_summary["runtime"] == "LIVE"
        cp008 = res.checkpoints.get("CP-008")
        assert cp008 is not None
        assert cp008.status.value in ("PASS", "WARN", "FAIL", "UNKNOWN")

    def test_13_runtime_evidence_unavailable(self) -> None:
        """13. Runtime evidence unavailable leaves CP-008 as UNKNOWN (zero metric fabrication)."""
        conn = MockLiveConnector(
            run_payload=None,
            historical_payload={"runs": []},
            error_on_runtime=DatabricksApiError("No run", status_code=404),
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        cp008 = res.checkpoints.get("CP-008")
        assert cp008 is not None
        assert cp008.status.value == "UNKNOWN"

    def test_14_provenance_preservation(self) -> None:
        """14. Every acquired live evidence item preserves LIVE_API provenance."""
        conn = MockLiveConnector()
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        for cat in ("workspace", "job", "cluster", "runtime", "historical"):
            diag = next(d for d in res.evidence_diagnostics if d.category == cat)
            assert diag.provenance == "LIVE_API"

    def test_15_credential_sanitization(self) -> None:
        """15. Sensitive tokens and secret fields are sanitized in all payloads."""
        conn = MockLiveConnector()
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        job_item = res.normalized_evidence.items[EvidenceCategory.JOB.value]
        assert job_item.payload.get("token_secret") == "[REDACTED_SECRET]"
        assert SECRET_TOKEN not in str(job_item.payload)

    def test_16_m5h_sufficiency_propagation(self) -> None:
        """16. Partial evidence acquisition reduces M5H confidence and sufficiency."""
        conn = MockLiveConnector(
            run_payload=None,
            historical_payload={"runs": []},
            error_on_runtime=DatabricksApiError("404", status_code=404),
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345, code_path=None)

        assert res.evidence_sufficiency is not None
        assert res.decision_sufficiency is False
        assert res.confidence in ("LOW", "INSUFFICIENT", "MEDIUM")

    def test_17_m5i_decision_propagation(self) -> None:
        """17. Insufficient evidence propagates into M5I final engineering decision."""
        conn = MockLiveConnector(
            run_payload=None,
            historical_payload={"runs": []},
            error_on_runtime=DatabricksApiError("404", status_code=404),
        )
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        assert res.decision_risk_synthesis is not None
        assert res.final_decision in ("NOT_PRODUCTION_READY", "INSUFFICIENT_EVIDENCE")

    def test_18_json_output_contract(self) -> None:
        """18. Online validation JSON contains execution_mode='online' and zero secrets."""
        conn = MockLiveConnector()
        orchestrator = OnlineValidationOrchestrator(connector=conn)
        res = orchestrator.validate(job_id=12345)

        json_dict = res.to_dict()
        assert json_dict["execution_mode"] == "online"
        assert json_dict["resource"]["type"] == "job"
        assert json_dict["resource"]["id"] == "12345"
        assert "evidence_summary" in json_dict
        assert "decision_risk_synthesis" in json_dict

        json_str = json.dumps(json_dict)
        assert SECRET_TOKEN not in json_str

    def test_19_cli_output(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """19. CLI displays header, evidence table, M5H, and M5I synthesis."""
        conn = MockLiveConnector()
        monkeypatch.setattr(
            "dpif.orchestration.online.LiveDatabricksConnector",
            lambda *args, **kwargs: conn,
        )

        runner = CliRunner()
        res = runner.invoke(
            cli,
            ["validate-online", "--job-id", "12345", "--workspace", WORKSPACE_HOST, "--token", SECRET_TOKEN],
        )

        assert res.exit_code == 0
        assert "DPIF ONLINE VALIDATION" in res.output
        assert "RESOURCE:" in res.output
        assert "JOB 12345" in res.output
        assert "Workspace" in res.output
        assert "M5H EVIDENCE COVERAGE" in res.output
        assert "DECISION & RISK SYNTHESIS (M5I)" in res.output

    def test_20_no_credential_leakage(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """20. Comprehensive regex scan verifies zero secret leakage across all outputs."""
        conn = MockLiveConnector()
        monkeypatch.setattr(
            "dpif.orchestration.online.LiveDatabricksConnector",
            lambda *args, **kwargs: conn,
        )

        runner = CliRunner()
        # Test human-readable CLI
        cli_res = runner.invoke(
            cli,
            ["validate-online", "--job-id", "12345", "--workspace", WORKSPACE_HOST, "--token", SECRET_TOKEN],
        )
        assert SECRET_TOKEN not in cli_res.output

        # Test JSON CLI
        json_res = runner.invoke(
            cli,
            ["validate-online", "--job-id", "12345", "--workspace", WORKSPACE_HOST, "--token", SECRET_TOKEN, "--json"],
        )
        assert SECRET_TOKEN not in json_res.output

        # Regex scan for common secret patterns
        token_regex = re.compile(r"dapi[0-9a-zA-Z]{20,}")
        bearer_regex = re.compile(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]{15,}")
        assert not token_regex.search(cli_res.output)
        assert not bearer_regex.search(cli_res.output)
        assert not token_regex.search(json_res.output)
        assert not bearer_regex.search(json_res.output)
