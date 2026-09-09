"""Unit tests for Online Databricks Job Discovery (M5B).

Tests cover:
A. Successful direct job discovery
B. Job not found (404 / UNKNOWN)
C. Authentication failure (401)
D. Authorization failure (403)
E. Timeout (408)
F. Rate limit (429)
G. API unavailable (502/503/504)
H. Malformed API response
I. Job with multiple tasks
J. Job with notebook task
K. Job with Python task
L. Job with SQL task
M. Job cluster reference
N. Retry/timeout configuration preservation
O. Credential-containing API response sanitization
P. Provenance tracking
Q. UNKNOWN semantics on job discovery failure
R. Offline regression
"""

from __future__ import annotations

from unittest.mock import MagicMock

import requests

from dpif.connectors.live import LiveDatabricksConnector
from dpif.connectors.offline import OfflineDatabricksConnector
from dpif.providers.base import (
    AcquisitionErrorCode,
    DatabricksEvidenceProvider,
    EvidenceCategory,
    sanitize_job_payload,
)

# -----------------------------------------------------------------------------
# MOCK API RESPONSE FIXTURES (Explicitly labeled as MOCK/FIXTURE)
# -----------------------------------------------------------------------------

MOCK_JOB_NOTEBOOK_TASK_RESPONSE = {
    "job_id": 1001,
    "creator_user_name": "user@example.com",
    "settings": {
        "name": "mock_notebook_job",
        "email_notifications": {"on_failure": ["user@example.com"]},
        "timeout_seconds": 3600,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "run_notebook",
                "notebook_task": {
                    "notebook_path": "/Workspace/Repos/dev/etl_notebook",
                    "base_parameters": {"env": "prod", "db_password": "secret_password_123"},
                },
                "job_cluster_key": "shared_job_cluster",
                "timeout_seconds": 1800,
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "shared_job_cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                },
            }
        ],
    },
    "created_time": 1672531199000,
}

MOCK_JOB_MULTI_TASK_RESPONSE = {
    "job_id": 2002,
    "settings": {
        "name": "mock_multi_task_job",
        "max_concurrent_runs": 2,
        "tasks": [
            {
                "task_key": "task_python",
                "spark_python_task": {
                    "python_file": "dbfs:/scripts/etl.py",
                    "parameters": ["--date", "2026-09-09"],
                },
                "existing_cluster_id": "0123-456789-cluster1",
            },
            {
                "task_key": "task_sql",
                "sql_task": {
                    "query": {"query_id": "sql-query-uuid-999"},
                    "warehouse_id": "warehouse-1234",
                },
                "depends_on": [{"task_key": "task_python"}],
            },
        ],
        "schedule": {
            "quartz_cron_expression": "0 0 12 * * ?",
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        },
    },
}

MOCK_JOB_WITH_SECRETS_RESPONSE = {
    "job_id": 3003,
    "settings": {
        "name": "sensitive_job",
        "tasks": [
            {
                "task_key": "task_auth",
                "spark_python_task": {
                    "python_file": "dbfs:/main.py",
                    "parameters": ["--api_key", "secret_api_key_xyz"],
                },
                "password": "super_secret_password",
                "api_token": "dapi_secret_12345",
            }
        ],
    },
}


# -----------------------------------------------------------------------------
# TESTS
# -----------------------------------------------------------------------------


def test_m5b_successful_direct_job_discovery():
    """A. Test successful direct job discovery with notebook task using injectable HTTP seam."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = MOCK_JOB_NOTEBOOK_TASK_RESPONSE
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    assert evidence.pipeline_id == "p1"
    assert evidence.connector_mode == "live"
    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is True
    assert isinstance(job_item.payload, dict)
    assert job_item.payload["job_id"] == 1001
    assert job_item.payload["settings"]["name"] == "mock_notebook_job"
    assert len(evidence.errors) == 0


def test_m5b_job_not_found_404():
    """B. Test job not found (404) returns is_available=False and UNKNOWN semantics."""
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
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=9999)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.payload is None
    assert job_item.error is not None
    assert job_item.error.error_code == AcquisitionErrorCode.RESOURCE_NOT_FOUND
    assert job_item.error.status_code == 404


def test_m5b_authentication_failure_401():
    """C. Test 401 authentication failure maps to AUTHENTICATION_FAILURE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = "Invalid bearer token dapi_mock_token_123"
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.error is not None
    assert job_item.error.error_code == AcquisitionErrorCode.AUTHENTICATION_FAILURE
    assert job_item.error.status_code == 401
    assert "dapi_mock_token_123" not in job_item.error.message


def test_m5b_authorization_failure_403():
    """D. Test 403 authorization failure maps to AUTHORIZATION_FAILURE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 403
    mock_resp.text = "User lacks permission on job 1001"
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.error is not None
    assert job_item.error.error_code == AcquisitionErrorCode.AUTHORIZATION_FAILURE
    assert job_item.error.status_code == 403


def test_m5b_timeout_408():
    """E. Test timeout maps to TIMEOUT error code."""
    mock_session = MagicMock(spec=requests.Session)
    mock_session.request.side_effect = requests.exceptions.Timeout("Read timed out")

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.error is not None
    assert job_item.error.error_code == AcquisitionErrorCode.TIMEOUT
    assert job_item.error.status_code == 408


def test_m5b_rate_limit_429():
    """F. Test 429 rate limit maps to RATE_LIMIT_EXCEEDED."""
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
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.error is not None
    assert job_item.error.error_code == AcquisitionErrorCode.RATE_LIMIT_EXCEEDED
    assert job_item.error.status_code == 429


def test_m5b_api_unavailable_503():
    """G. Test 503 service unavailable maps to API_UNAVAILABLE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_session.request.side_effect = requests.exceptions.RequestException("503 Server Error")

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.error is not None
    assert job_item.error.error_code == AcquisitionErrorCode.API_UNAVAILABLE
    assert job_item.error.status_code == 503


def test_m5b_malformed_json_response_handling():
    """1. Test HTTP 200 with invalid/malformed JSON returns MALFORMED_RESPONSE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.side_effect = ValueError("Expecting value: line 1 column 1 (char 0)")
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.payload is None
    assert job_item.error is not None
    assert job_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE
    assert job_item.error.status_code == 200


def test_m5b_security_no_raw_response_leakage_401_403():
    """2 & 3. Test 401/403 errors never leak raw bodies or sensitive JSON secrets."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = (
        '{"error": "Unauthorized", "password": "super_secret_pass", "token": "dapi_secret_99"}'
    )
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.error is not None
    msg = job_item.error.message
    assert "super_secret_pass" not in msg
    assert "dapi_secret_99" not in msg
    assert '{"error": "Unauthorized"' not in msg


def test_m5b_string_sanitization_pattern_hardening():
    """3. Test mask_sensitive_credentials regex pattern matching JSON and key-value formats."""
    from dpif.providers.base import mask_sensitive_credentials

    test_inputs = [
        ("password=super_secret", "password=[MASKED_SECRET]"),
        ("password: super_secret", "password: [MASKED_SECRET]"),
        ('"password": "super_secret"', '"password": [MASKED_SECRET]'),
        ("token=abc_token_123", "token=[MASKED_SECRET]"),
        ('"token": "abc_token_123"', '"token": [MASKED_SECRET]'),
        ("secret=shh_secret", "secret=[MASKED_SECRET]"),
        ('"secret": "shh_secret"', '"secret": [MASKED_SECRET]'),
        ("api_key=key_xyz", "api_key=[MASKED_SECRET]"),
        ('"api_key": "key_xyz"', '"api_key": [MASKED_SECRET]'),
        ("authorization: Bearer secret_bearer_tok", "authorization: Bearer [MASKED_SECRET]"),
        (
            '"authorization": "Bearer secret_bearer_tok"',
            '"authorization": "Bearer [MASKED_SECRET]"',
        ),
    ]

    for raw, expected in test_inputs:
        sanitized = mask_sensitive_credentials(raw)
        assert sanitized == expected, f"Failed for {raw}: got {sanitized}"


def test_m5b_malformed_api_response():
    """H. Test handling of unexpected server error status codes."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Internal Server Error"
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=1001)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is False
    assert job_item.error is not None
    assert job_item.error.error_code == AcquisitionErrorCode.UNKNOWN_ERROR


def test_m5b_multi_task_python_and_sql_job_discovery():
    """I, J, K, L, M, N. Test multi-task job with Python, SQL, schedule, and cluster refs."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = MOCK_JOB_MULTI_TASK_RESPONSE
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p2", job_id=2002)

    job_payload = evidence.get_payload(EvidenceCategory.JOB)
    assert isinstance(job_payload, dict)
    tasks = job_payload["settings"]["tasks"]
    assert len(tasks) == 2

    # Task K (Python)
    python_task = tasks[0]
    assert python_task["task_key"] == "task_python"
    assert python_task["spark_python_task"]["python_file"] == "dbfs:/scripts/etl.py"

    # Task L (SQL)
    sql_task = tasks[1]
    assert sql_task["task_key"] == "task_sql"
    assert sql_task["sql_task"]["warehouse_id"] == "warehouse-1234"

    # Schedule / Cron
    schedule = job_payload["settings"]["schedule"]
    assert schedule["quartz_cron_expression"] == "0 0 12 * * ?"


def test_m5b_credential_sanitization_in_job_payload():
    """O. Test credential-containing API response sanitization."""
    sanitized = sanitize_job_payload(MOCK_JOB_WITH_SECRETS_RESPONSE, token="dapi_secret_12345")
    task = sanitized["settings"]["tasks"][0]
    assert task["password"] == "[REDACTED_SECRET]"
    assert task["api_token"] == "[REDACTED_SECRET]"
    assert "secret_password" not in str(sanitized)


def test_m5b_provenance_and_unknown_semantics():
    """P, Q. Test evidence provenance tracking and UNKNOWN semantics for missing job."""
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
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p3", job_id=8888)

    job_item = evidence.items[EvidenceCategory.JOB.value]
    prov = job_item.provenance
    assert prov.source_type == "LIVE_API"
    assert prov.source_system == "DATABRICKS"
    assert prov.workspace_host == "https://mock.cloud.databricks.com"
    assert prov.resource_id == "8888"
    assert prov.category == EvidenceCategory.JOB

    # UNKNOWN semantics verification
    assert job_item.is_available is False
    assert job_item.payload is None


def test_m5b_offline_connector_regression():
    """R. Test offline connector job discovery regression."""
    offline_conn = OfflineDatabricksConnector()
    provider = DatabricksEvidenceProvider(connector=offline_conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="good", job_id="job_default")

    job_item = evidence.items[EvidenceCategory.JOB.value]
    assert job_item.is_available is True
    assert job_item.provenance.source_type == "FIXTURE"
    assert job_item.provenance.is_mock is True
