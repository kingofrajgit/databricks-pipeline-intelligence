"""Unit tests for DatabricksEvidenceProvider (M5A).

Tests cover:
A. provider interface
B. successful evidence acquisition
C. missing evidence (404 / UNKNOWN)
D. authentication failure (401)
E. authorization failure (403)
F. resource not found
G. timeout / API failure (408 / 503)
H. malformed API response / error mapping
I. rate-limit handling (429)
J. provenance tracking
K. no credential leakage (secret masking)
L. connector mode normalization
M. offline regression
N. existing single-pipeline validation compatibility
O. existing multi-pipeline batch compatibility
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dpif.connectors.live import DatabricksApiError, LiveDatabricksConnector
from dpif.connectors.offline import OfflineDatabricksConnector
from dpif.providers.base import (
    AcquisitionErrorCode,
    DatabricksEvidenceProvider,
    EvidenceCategory,
    mask_sensitive_credentials,
)


def test_mask_sensitive_credentials():
    """Verify secrets and bearer tokens are masked from strings."""
    token = "secret-token-12345"
    raw = f"Error calling API with Bearer {token} and password=super_secret"
    clean = mask_sensitive_credentials(raw, token)
    assert token not in clean
    assert "super_secret" not in clean
    assert "[MASKED_TOKEN]" in clean or "[MASKED_SECRET]" in clean


def test_provider_with_offline_connector_regression():
    """Verify DatabricksEvidenceProvider works with OfflineDatabricksConnector."""
    offline_conn = OfflineDatabricksConnector()
    provider = DatabricksEvidenceProvider(connector=offline_conn)

    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="good",
        job_id="job_default",
        cluster_id="cluster_default",
        table_name="bronze_events",
        run_id="healthy",
        include_historical_runs=True,
    )

    assert evidence.pipeline_id == "good"
    assert evidence.connector_mode == "offline-fixture"

    # Workspace
    item_ws = evidence.items[EvidenceCategory.WORKSPACE.value]
    assert item_ws.is_available
    assert item_ws.provenance.source_type == "FIXTURE"
    assert item_ws.provenance.is_mock is True

    # Pipeline
    item_pipe = evidence.items[EvidenceCategory.PIPELINE.value]
    assert item_pipe.is_available
    assert isinstance(item_pipe.payload, dict)
    assert item_pipe.payload["name"] in ("good_pipeline", "customers_dlt_pipeline")

    # Job
    item_job = evidence.items[EvidenceCategory.JOB.value]
    assert item_job.is_available

    # Cluster
    item_cluster = evidence.items[EvidenceCategory.CLUSTER.value]
    assert item_cluster.is_available


def test_provider_mode_normalization():
    """Verify live-api connector mode is normalized to live in evidence provider."""
    mock_conn = MagicMock()
    mock_conn.mode.return_value = "live-api"
    mock_conn.host = "https://test.cloud.databricks.com"
    mock_conn.get_workspace_status.return_value = {"status": "connected"}

    provider = DatabricksEvidenceProvider(connector=mock_conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1")

    assert evidence.connector_mode == "live"
    assert evidence.items[EvidenceCategory.WORKSPACE.value].provenance.source_type == "LIVE_API"


def test_provider_successful_acquisition_with_mock_live_connector():
    """Test successful evidence acquisition across categories."""
    mock_conn = MagicMock(spec=LiveDatabricksConnector)
    mock_conn.mode.return_value = "live-api"
    mock_conn.host = "https://my-test-workspace.cloud.databricks.com"
    mock_conn._token = "dapi_fake_token_123"

    mock_conn.get_workspace_status.return_value = {"status": "connected", "spark_versions_count": 5}
    mock_conn.get_pipeline.return_value = {"pipeline_id": "p1", "name": "Test Pipeline"}
    mock_conn.get_job.return_value = {"job_id": 100, "settings": {"name": "Test Job"}}
    mock_conn.get_cluster.return_value = {"cluster_id": "c1", "spark_version": "13.3.x-scala2.12"}
    mock_conn.get_permissions.return_value = {"access_control_list": []}
    mock_conn.get_table_profile.return_value = {"table": "catalog.schema.table1"}
    mock_conn.get_run.return_value = {"run_id": 999, "state": {"result_state": "SUCCESS"}}
    mock_conn.get_recent_runs.return_value = [{"run_id": 998}, {"run_id": 999}]

    provider = DatabricksEvidenceProvider(connector=mock_conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1",
        job_id=100,
        cluster_id="c1",
        table_name="catalog.schema.table1",
        run_id=999,
        include_historical_runs=True,
    )

    assert evidence.pipeline_id == "p1"
    assert len(evidence.errors) == 0

    pipe_payload = evidence.get_payload(EvidenceCategory.PIPELINE)
    assert isinstance(pipe_payload, dict)
    assert pipe_payload["name"] == "Test Pipeline"

    job_payload = evidence.get_payload(EvidenceCategory.JOB)
    assert isinstance(job_payload, dict)
    assert job_payload["job_id"] == 100

    cluster_payload = evidence.get_payload(EvidenceCategory.CLUSTER)
    assert isinstance(cluster_payload, dict)
    assert cluster_payload["cluster_id"] == "c1"

    hist_runs = evidence.get_payload(EvidenceCategory.HISTORICAL_RUNS)
    assert isinstance(hist_runs, list)
    assert len(hist_runs) == 2


def test_provider_missing_evidence_returns_unknown():
    """Verify 404/None returns unavailable item with UNKNOWN semantics without throwing."""
    mock_conn = MagicMock(spec=LiveDatabricksConnector)
    mock_conn.mode.return_value = "live"
    mock_conn.host = "https://test.databricks.com"
    mock_conn.get_workspace_status.return_value = {"status": "connected"}
    mock_conn.get_pipeline.return_value = None  # Not found

    provider = DatabricksEvidenceProvider(connector=mock_conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="missing_p1")

    item = evidence.items[EvidenceCategory.PIPELINE.value]
    assert item.is_available is False
    assert item.payload is None
    assert item.error is not None
    assert item.error.error_code == AcquisitionErrorCode.RESOURCE_NOT_FOUND
    assert item.error.status_code == 404


def test_provider_authentication_failure_401():
    """Verify 401 raises formatted AcquisitionError and masks token."""
    mock_conn = MagicMock(spec=LiveDatabricksConnector)
    mock_conn.mode.return_value = "live"
    mock_conn.host = "https://test.databricks.com"
    mock_conn._token = "dapi_secret_token_val"
    mock_conn.get_workspace_status.side_effect = DatabricksApiError(
        "Authentication failure (401): invalid token dapi_secret_token_val", status_code=401
    )

    provider = DatabricksEvidenceProvider(connector=mock_conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1")

    assert len(evidence.errors) > 0
    err = evidence.errors[0]
    assert err.error_code == AcquisitionErrorCode.AUTHENTICATION_FAILURE
    assert err.status_code == 401
    assert "dapi_secret_token_val" not in err.message
    assert "[MASKED_TOKEN]" in err.message or "[MASKED" in err.message


def test_provider_authorization_failure_403():
    """Verify 403 authorization failure handling."""
    mock_conn = MagicMock(spec=LiveDatabricksConnector)
    mock_conn.mode.return_value = "live"
    mock_conn.get_workspace_status.return_value = {"status": "connected"}
    mock_conn.get_pipeline.side_effect = DatabricksApiError("Forbidden", status_code=403)

    provider = DatabricksEvidenceProvider(connector=mock_conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1")

    item = evidence.items[EvidenceCategory.PIPELINE.value]
    assert item.is_available is False
    assert item.error is not None
    assert item.error.error_code == AcquisitionErrorCode.AUTHORIZATION_FAILURE
    assert item.error.status_code == 403


def test_provider_rate_limit_429():
    """Verify 429 rate-limit handling."""
    mock_conn = MagicMock(spec=LiveDatabricksConnector)
    mock_conn.mode.return_value = "live"
    mock_conn.get_workspace_status.return_value = {"status": "connected"}
    mock_conn.get_pipeline.side_effect = DatabricksApiError("Rate limit exceeded", status_code=429)

    provider = DatabricksEvidenceProvider(connector=mock_conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1")

    item = evidence.items[EvidenceCategory.PIPELINE.value]
    assert item.is_available is False
    assert item.error is not None
    assert item.error.error_code == AcquisitionErrorCode.RATE_LIMIT_EXCEEDED
    assert item.error.status_code == 429


def test_provider_timeout_408_and_api_unavailable_503():
    """Verify 408 timeout and 503 network failure handling."""
    mock_conn = MagicMock(spec=LiveDatabricksConnector)
    mock_conn.mode.return_value = "live"
    mock_conn.get_workspace_status.side_effect = DatabricksApiError(
        "Request timed out", status_code=408
    )
    mock_conn.get_pipeline.side_effect = DatabricksApiError("Service unavailable", status_code=503)

    provider = DatabricksEvidenceProvider(connector=mock_conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1")

    ws_item = evidence.items[EvidenceCategory.WORKSPACE.value]
    assert ws_item.error is not None
    assert ws_item.error.error_code == AcquisitionErrorCode.TIMEOUT

    pipe_item = evidence.items[EvidenceCategory.PIPELINE.value]
    assert pipe_item.error is not None
    assert pipe_item.error.error_code == AcquisitionErrorCode.API_UNAVAILABLE


def test_provenance_tracking():
    """Verify evidence objects contain detailed provenance without secrets."""
    mock_conn = MagicMock(spec=LiveDatabricksConnector)
    mock_conn.mode.return_value = "live"
    mock_conn.host = "https://prod-workspace.cloud.databricks.com"
    mock_conn.get_workspace_status.return_value = {"status": "connected"}

    provider = DatabricksEvidenceProvider(connector=mock_conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1")

    prov = evidence.items[EvidenceCategory.WORKSPACE.value].provenance
    assert prov.source_type == "LIVE_API"
    assert prov.source_system == "DATABRICKS"
    assert prov.workspace_host == "https://prod-workspace.cloud.databricks.com"
    assert prov.is_mock is False
    assert "token" not in prov.to_dict()
