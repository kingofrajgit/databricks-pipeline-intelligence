"""Unit tests for M5D Online Databricks Runtime & Run Evidence Acquisition."""

from unittest.mock import MagicMock

import requests

from dpif.connectors.live import LiveDatabricksConnector
from dpif.providers import (
    AcquisitionErrorCode,
    DatabricksEvidenceProvider,
    EvidenceCategory,
    sanitize_job_payload,
)

MOCK_COMPLETED_SUCCESS_RUN = {
    "run_id": 999111,
    "run_name": "prod_monthly_etl_run",
    "job_id": 4567,
    "run_page_url": "https://mock.cloud.databricks.com/#job/4567/run/999111",
    "creator_user_name": "data_eng_service_acct@company.com",
    "start_time": 1726000000000,
    "end_time": 1726000300000,
    "setup_duration": 15000,
    "execution_duration": 270000,
    "cleanup_duration": 15000,
    "trigger": "PERIODIC",
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
            "existing_cluster_id": "0123-456789-cluster1",
        }
    ],
    "spark_env_vars": {"DB_SECRET_KEY": "dapi_super_secret_key_123"},
}

MOCK_COMPLETED_FAILED_RUN = {
    "run_id": 999112,
    "run_name": "prod_monthly_etl_run_failed",
    "job_id": 4567,
    "start_time": 1726000000000,
    "end_time": 1726000060000,
    "execution_duration": 60000,
    "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "FAILED",
        "state_message": "Task extract_bronze failed due to OutOfMemoryError",
    },
}

MOCK_RUNNING_IN_PROGRESS_RUN = {
    "run_id": 999113,
    "run_name": "prod_monthly_etl_run_active",
    "job_id": 4567,
    "start_time": 1726000000000,
    "execution_duration": 120000,
    "state": {
        "life_cycle_state": "RUNNING",
        "state_message": "In progress...",
    },
}

MOCK_HISTORICAL_RUNS_LIST = [
    {
        "run_id": 999100,
        "job_id": 4567,
        "start_time": 1725900000000,
        "end_time": 1725900300000,
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
    },
    {
        "run_id": 999099,
        "job_id": 4567,
        "start_time": 1725800000000,
        "end_time": 1725800350000,
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
    },
]


def test_m5d_successful_run_discovery():
    """A, J, M, N, R. Test successful direct run discovery with timing and state metadata."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = MOCK_COMPLETED_SUCCESS_RUN
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999111)

    assert evidence.pipeline_id == "p1"
    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is True
    assert runtime_item.payload["run_id"] == 999111
    assert runtime_item.payload["state"]["result_state"] == "SUCCESS"
    assert runtime_item.payload["execution_duration"] == 270000
    assert runtime_item.provenance.source_type == "LIVE_API"


def test_m5d_run_not_found_404():
    """B. Test 404 response for run produces RESOURCE_NOT_FOUND error."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999999)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is False
    assert runtime_item.payload is None
    assert runtime_item.error is not None
    assert runtime_item.error.error_code == AcquisitionErrorCode.RESOURCE_NOT_FOUND


def test_m5d_authentication_failure_401():
    """C. Test 401 response produces AUTHENTICATION_FAILURE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="invalid_token",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999111)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is False
    assert runtime_item.error.error_code == AcquisitionErrorCode.AUTHENTICATION_FAILURE


def test_m5d_authorization_failure_403():
    """D. Test 403 response produces AUTHORIZATION_FAILURE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 403
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_restricted",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999111)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is False
    assert runtime_item.error.error_code == AcquisitionErrorCode.AUTHORIZATION_FAILURE


def test_m5d_timeout_408():
    """E. Test timeout exception produces TIMEOUT error."""
    mock_session = MagicMock(spec=requests.Session)
    mock_session.request.side_effect = requests.exceptions.Timeout("Connection timed out")

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999111)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is False
    assert runtime_item.error.error_code == AcquisitionErrorCode.TIMEOUT


def test_m5d_rate_limit_429():
    """F. Test 429 response produces RATE_LIMIT_EXCEEDED."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 429
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999111)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is False
    assert runtime_item.error.error_code == AcquisitionErrorCode.RATE_LIMIT_EXCEEDED


def test_m5d_api_unavailable_503():
    """G. Test 503 response produces API_UNAVAILABLE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 503
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999111)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is False
    assert runtime_item.error.error_code == AcquisitionErrorCode.API_UNAVAILABLE


def test_m5d_malformed_json_response():
    """H. Test HTTP 200 + invalid JSON produces MALFORMED_RESPONSE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.side_effect = ValueError("Invalid JSON")
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999111)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is False
    assert runtime_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE


def test_m5d_unexpected_successful_response_structure():
    """I. Test HTTP 200 + unexpected response shape (no run_id) produces MALFORMED_RESPONSE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"hello": "world"}
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999111)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is False
    assert runtime_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE


def test_m5d_completed_failed_run():
    """K. Test completed failed run produces valid RUNTIME evidence with FAILED state."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = MOCK_COMPLETED_FAILED_RUN
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999112)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is True
    assert runtime_item.payload["state"]["result_state"] == "FAILED"


def test_m5d_running_in_progress_run():
    """L, S. Test running in-progress run with no result_state remains available."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = MOCK_RUNNING_IN_PROGRESS_RUN
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", run_id=999113)

    runtime_item = evidence.items[EvidenceCategory.RUNTIME.value]
    assert runtime_item.is_available is True
    assert runtime_item.payload["state"]["life_cycle_state"] == "RUNNING"
    assert "result_state" not in runtime_item.payload["state"]


def test_m5d_historical_runs():
    """O. Test acquiring historical runs list under EvidenceCategory.HISTORICAL_RUNS."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"runs": MOCK_HISTORICAL_RUNS_LIST}
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", job_id=4567, include_historical_runs=True
    )

    hist_item = evidence.items[EvidenceCategory.HISTORICAL_RUNS.value]
    assert hist_item.is_available is True
    assert isinstance(hist_item.payload, list)
    assert len(hist_item.payload) == 2
    assert hist_item.payload[0]["run_id"] == 999100


def test_m5d_empty_historical_run_result():
    """P. Test empty historical run result ([] runs) returns is_available=True with empty list."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"runs": []}
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", job_id=4567, include_historical_runs=True
    )

    hist_item = evidence.items[EvidenceCategory.HISTORICAL_RUNS.value]
    assert hist_item.is_available is True
    assert hist_item.payload == []


def test_m5d_credential_sanitization_in_run_payload():
    """Q. Test sensitive credentials in run payload spark_env_vars are masked."""
    sanitized = sanitize_job_payload(
        MOCK_COMPLETED_SUCCESS_RUN, token="dapi_super_secret_key_123"
    )
    assert sanitized["spark_env_vars"]["DB_SECRET_KEY"] == "[REDACTED_SECRET]"
    assert sanitized["run_id"] == 999111
    assert sanitized["job_id"] == 4567


def test_m5d_m5b_regression():
    """T. Verify M5B Job Discovery functionality remains fully intact."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "job_id": 4567,
        "settings": {"name": "test_job"},
    }
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=4567)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is True
    assert job_item.payload["job_id"] == 4567


def test_m5d_m5c_regression():
    """U. Verify M5C Cluster Discovery structural validation remains fully intact."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"cluster_name": "no_cluster_id"}
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="synthetic-cluster-123"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE
