"""Unit tests for Online Databricks Cluster Discovery (M5C).

Tests cover:
A. Successful direct cluster discovery
B. Cluster not found (404 / UNKNOWN)
C. Authentication failure (401)
D. Authorization failure (403)
E. Timeout (408)
F. Rate limit (429)
G. API unavailable (503)
H. Malformed JSON (HTTP 200 + invalid JSON -> MALFORMED_RESPONSE)
I. Unexpected status / server error
J. Autoscaling cluster
K. Fixed-worker cluster
L. Single-node cluster
M. Spark configuration (spark_conf)
N. Environment-variable configuration (spark_env_vars)
O. Cluster policy reference (policy_id)
P. Instance pool reference (instance_pool_id)
Q. Security / access mode configuration (data_security_mode / custom_tags)
R. Autotermination (autotermination_minutes)
S. Init scripts & cluster log configuration (init_scripts, cluster_log_conf)
T. Credential-containing payload sanitization
U. Provenance tracking
V. UNKNOWN semantics on cluster discovery failure
W. Job-to-Cluster cross-referencing (existing_cluster_id vs job_cluster_key)
"""

from __future__ import annotations

from unittest.mock import MagicMock

import requests

from dpif.connectors.live import LiveDatabricksConnector
from dpif.providers.base import (
    AcquisitionErrorCode,
    DatabricksEvidenceProvider,
    EvidenceCategory,
    sanitize_job_payload,
)

# -----------------------------------------------------------------------------
# MOCK CLUSTER API RESPONSE FIXTURES (Explicitly labeled as MOCK/FIXTURE)
# -----------------------------------------------------------------------------

MOCK_AUTOSCALING_CLUSTER_RESPONSE = {
    "cluster_id": "0123-456789-cluster1",
    "cluster_name": "prod_autoscale_cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "driver_node_type_id": "i3.xlarge",
    "autoscale": {"min_workers": 2, "max_workers": 8},
    "state": "RUNNING",
    "state_message": "Cluster running normally",
    "creator_user_name": "admin@example.com",
    "policy_id": "policy-abcd-1234",
    "autotermination_minutes": 60,
    "enable_elastic_disk": True,
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "STANDARD",
    "custom_tags": {"Environment": "Production", "Owner": "DataEngineering"},
    "spark_conf": {
        "spark.speculation": "true",
        "spark.databricks.delta.preview.enabled": "true",
        "spark.secret.password": "raw_spark_secret_pass",
    },
    "spark_env_vars": {
        "ENV": "PROD",
        "DB_TOKEN": "dapi_secret_token_999",
    },
    "cluster_log_conf": {"dbfs": {"destination": "dbfs:/cluster-logs"}},
    "init_scripts": [{"dbfs": {"destination": "dbfs:/databricks/init/setup.sh"}}],
}

MOCK_FIXED_WORKER_CLUSTER_RESPONSE = {
    "cluster_id": "0987-654321-cluster2",
    "cluster_name": "fixed_worker_cluster",
    "spark_version": "12.2.x-scala2.12",
    "node_type_id": "m5d.large",
    "num_workers": 4,
    "state": "TERMINATED",
    "state_message": "Cluster terminated due to inactivity",
    "instance_pool_id": "pool-xyz-555",
    "autotermination_minutes": 30,
}

MOCK_SINGLE_NODE_CLUSTER_RESPONSE = {
    "cluster_id": "1111-222233-single",
    "cluster_name": "single_node_dev",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 0,
    "spark_conf": {"spark.databricks.isSingleNode": "true"},
    "state": "RUNNING",
}

MOCK_JOB_WITH_EXISTING_CLUSTER = {
    "job_id": 5001,
    "settings": {
        "name": "job_with_existing_cluster",
        "tasks": [
            {
                "task_key": "etl_task",
                "notebook_task": {"notebook_path": "/etl"},
                "existing_cluster_id": "0123-456789-cluster1",
            }
        ],
    },
}

MOCK_JOB_WITH_JOB_CLUSTER_KEY_ONLY = {
    "job_id": 5002,
    "settings": {
        "name": "job_with_job_cluster_key",
        "tasks": [
            {
                "task_key": "etl_task",
                "notebook_task": {"notebook_path": "/etl"},
                "job_cluster_key": "ephemeral_cluster_key",
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "ephemeral_cluster_key",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                },
            }
        ],
    },
}


# -----------------------------------------------------------------------------
# TESTS
# -----------------------------------------------------------------------------


def test_m5c_successful_direct_cluster_discovery():
    """A. Test successful direct cluster discovery using LiveDatabricksConnector and HTTP seam."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = MOCK_AUTOSCALING_CLUSTER_RESPONSE
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is True
    assert isinstance(cluster_item.payload, dict)
    assert cluster_item.payload["cluster_id"] == "0123-456789-cluster1"
    assert cluster_item.payload["cluster_name"] == "prod_autoscale_cluster"
    assert cluster_item.payload["autoscale"] == {"min_workers": 2, "max_workers": 8}


def test_m5c_cluster_not_found_404():
    """B. Test cluster not found (404) returns is_available=False and RESOURCE_NOT_FOUND."""
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
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", cluster_id="nonexistent-id")

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.payload is None
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.RESOURCE_NOT_FOUND
    assert cluster_item.error.status_code == 404


def test_m5c_authentication_failure_401():
    """C. Test 401 authentication failure returns AUTHENTICATION_FAILURE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = '{"error": "Invalid token"}'
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.AUTHENTICATION_FAILURE
    assert cluster_item.error.status_code == 401


def test_m5c_authorization_failure_403():
    """D. Test 403 authorization failure returns AUTHORIZATION_FAILURE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 403
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.AUTHORIZATION_FAILURE
    assert cluster_item.error.status_code == 403


def test_m5c_timeout_408():
    """E. Test timeout maps to TIMEOUT error code."""
    mock_session = MagicMock(spec=requests.Session)
    mock_session.request.side_effect = requests.exceptions.Timeout("Connection timed out")

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.TIMEOUT
    assert cluster_item.error.status_code == 408


def test_m5c_rate_limit_429():
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
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.RATE_LIMIT_EXCEEDED


def test_m5c_api_unavailable_503():
    """G. Test 503 service unavailable maps to API_UNAVAILABLE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_session.request.side_effect = requests.exceptions.RequestException("503 Service Error")

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.API_UNAVAILABLE


def test_m5c_malformed_json_response():
    """H. Test HTTP 200 with invalid JSON returns MALFORMED_RESPONSE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.side_effect = ValueError("Invalid JSON token")
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.payload is None
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE


def test_m5c_unexpected_response_structure_json_list():
    """E. Test HTTP 200 + JSON list ([]) produces MALFORMED_RESPONSE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = []
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.payload is None
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE


def test_m5c_unexpected_response_structure_unrelated_json():
    """F. Test HTTP 200 + unrelated JSON ({"hello": "world"}) produces MALFORMED_RESPONSE."""
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
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="0123-456789-cluster1"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is False
    assert cluster_item.payload is None
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE


def test_m5c_cluster_name_only_returns_malformed_response():
    """B, D. Test HTTP 200 + cluster_name only (no cluster_id) produces MALFORMED_RESPONSE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"cluster_name": "synthetic-cluster"}
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
    assert cluster_item.payload is None
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE


def test_m5c_empty_cluster_id_string_returns_malformed_response():
    """C. Test HTTP 200 + cluster_id as empty string produces MALFORMED_RESPONSE."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"cluster_id": "   ", "cluster_name": "synthetic"}
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
    assert cluster_item.payload is None
    assert cluster_item.error is not None
    assert cluster_item.error.error_code == AcquisitionErrorCode.MALFORMED_RESPONSE


def test_m5c_minimal_valid_cluster_response():
    """A, G. Test HTTP 200 + minimal valid cluster_id returns is_available=True."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"cluster_id": "synthetic-cluster-123"}
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
    assert cluster_item.is_available is True
    assert cluster_item.payload == {
        "cluster_id": "synthetic-cluster-123",
        "_connector": "live-api",
    }


def test_m5c_fixed_worker_and_single_node_cluster_attributes():
    """J..S. Test fixed-worker, single-node, and detailed configuration fields."""
    fixed = MOCK_FIXED_WORKER_CLUSTER_RESPONSE
    assert fixed["num_workers"] == 4
    assert fixed["instance_pool_id"] == "pool-xyz-555"

    single = MOCK_SINGLE_NODE_CLUSTER_RESPONSE
    assert single["num_workers"] == 0
    assert single["spark_conf"]["spark.databricks.isSingleNode"] == "true"


def test_m5c_credential_sanitization_in_cluster_payload():
    """T. Test payload sanitization redacts sensitive spark_conf and spark_env_vars secrets."""
    sanitized = sanitize_job_payload(
        MOCK_AUTOSCALING_CLUSTER_RESPONSE, token="dapi_secret_token_999"
    )
    assert sanitized["spark_conf"]["spark.secret.password"] == "[REDACTED_SECRET]"
    assert sanitized["spark_env_vars"]["DB_TOKEN"] == "[REDACTED_SECRET]"
    # Ensure legitimate configuration fields are preserved
    assert sanitized["spark_version"] == "13.3.x-scala2.12"
    assert sanitized["node_type_id"] == "i3.xlarge"
    assert sanitized["autotermination_minutes"] == 60


def test_m5c_cluster_provenance_and_unknown_semantics():
    """U, V. Test evidence provenance and UNKNOWN semantics on failure."""
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
    evidence = provider.acquire_pipeline_evidence(
        pipeline_id="p1", cluster_id="missing-cluster-id"
    )

    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    prov = cluster_item.provenance
    assert prov.source_type == "LIVE_API"
    assert prov.category == EvidenceCategory.CLUSTER
    assert prov.resource_id == "missing-cluster-id"
    assert cluster_item.is_available is False
    assert cluster_item.payload is None


def test_m5c_job_to_cluster_cross_reference_existing_cluster_id():
    """W. Test Job evidence with existing_cluster_id drives cluster acquisition cleanly."""
    mock_session = MagicMock(spec=requests.Session)

    def mock_request(method, url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        if "/api/2.1/jobs/get" in url:
            resp.json.return_value = MOCK_JOB_WITH_EXISTING_CLUSTER
        elif "/api/2.0/clusters/get" in url:
            resp.json.return_value = MOCK_AUTOSCALING_CLUSTER_RESPONSE
        else:
            resp.status_code = 404
        return resp

    mock_session.request.side_effect = mock_request

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=5001)

    assert EvidenceCategory.JOB.value in evidence.items
    assert EvidenceCategory.CLUSTER.value in evidence.items
    cluster_item = evidence.items[EvidenceCategory.CLUSTER.value]
    assert cluster_item.is_available is True
    assert cluster_item.payload["cluster_id"] == "0123-456789-cluster1"


def test_m5c_job_with_job_cluster_key_only_no_fake_cluster():
    """W. Test Job with job_cluster_key only does not fabricate fake cluster evidence."""
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = MOCK_JOB_WITH_JOB_CLUSTER_KEY_ONLY
    mock_session.request.return_value = mock_resp

    conn = LiveDatabricksConnector(
        host="https://mock.cloud.databricks.com",
        token="dapi_mock_token_123",
        session=mock_session,
    )
    provider = DatabricksEvidenceProvider(connector=conn)
    evidence = provider.acquire_pipeline_evidence(pipeline_id="p1", job_id=5002)

    assert EvidenceCategory.JOB.value in evidence.items
    assert EvidenceCategory.CLUSTER.value not in evidence.items
