"""Online Databricks Evidence Provider abstractions and models (M5A).

Decouples online evidence acquisition from downstream CP-001..CP-024 rule evaluation.
Acquires normalized evidence from Databricks API via existing LiveDatabricksConnector.
Strictly enforces UNKNOWN semantics on missing evidence and redacts sensitive credentials.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dpif.connectors.base import DatabricksConnector
from dpif.connectors.live import DatabricksApiError, LiveDatabricksConnector

logger = logging.getLogger(__name__)


class EvidenceCategory(StrEnum):
    """Supported evidence acquisition categories."""

    WORKSPACE = "workspace"
    JOB = "job"
    PIPELINE = "pipeline"
    CLUSTER = "cluster"
    PERMISSIONS = "permissions"
    CODE = "code"
    TABLE_PROFILE = "table_profile"
    RUNTIME = "runtime"
    HISTORICAL_RUNS = "historical_runs"


class AcquisitionErrorCode(StrEnum):
    """Structured online acquisition error codes."""

    AUTHENTICATION_FAILURE = "AUTHENTICATION_FAILURE"
    AUTHORIZATION_FAILURE = "AUTHORIZATION_FAILURE"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    API_UNAVAILABLE = "API_UNAVAILABLE"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    TIMEOUT = "TIMEOUT"
    MALFORMED_RESPONSE = "MALFORMED_RESPONSE"
    UNSUPPORTED_CATEGORY = "UNSUPPORTED_CATEGORY"
    NOT_CONFIGURED = "NOT_CONFIGURED"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


class AcquisitionError(BaseModel):
    """Safe, structured error details for failed online evidence acquisition."""

    model_config = ConfigDict(populate_by_name=True)

    category: EvidenceCategory
    error_code: AcquisitionErrorCode
    message: str
    status_code: int | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category.value,
            "error_code": self.error_code.value,
            "message": self.message,
            "status_code": self.status_code,
            "timestamp": self.timestamp.isoformat(),
        }


class EvidenceProvenance(BaseModel):
    """Metadata tracking the origin and chain of custody for acquired evidence."""

    model_config = ConfigDict(populate_by_name=True)

    source_type: str  # e.g., "LIVE_API", "FIXTURE"
    source_system: str  # e.g., "DATABRICKS"
    category: EvidenceCategory
    acquired_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    workspace_host: str | None = None
    resource_id: str | None = None
    is_mock: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "source_system": self.source_system,
            "category": self.category.value,
            "acquired_at": self.acquired_at.isoformat(),
            "workspace_host": self.workspace_host,
            "resource_id": self.resource_id,
            "is_mock": self.is_mock,
        }


class NormalizedEvidenceItem(BaseModel):
    """Container for an acquired evidence payload with provenance and status."""

    model_config = ConfigDict(populate_by_name=True)

    category: EvidenceCategory
    provenance: EvidenceProvenance
    payload: dict[str, Any] | list[dict[str, Any]] | None = None
    is_available: bool = True
    error: AcquisitionError | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category.value,
            "provenance": self.provenance.to_dict(),
            "payload": self.payload,
            "is_available": self.is_available,
            "error": self.error.to_dict() if self.error else None,
        }


class NormalizedPipelineEvidence(BaseModel):
    """Consolidated online evidence bundle ready to feed the downstream engine."""

    model_config = ConfigDict(populate_by_name=True)

    pipeline_id: str
    connector_mode: str = "live-api"
    items: dict[str, NormalizedEvidenceItem] = Field(default_factory=dict)
    errors: list[AcquisitionError] = Field(default_factory=list)
    acquired_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def get_payload(self, category: EvidenceCategory) -> Any | None:
        item = self.items.get(category.value)
        if item and item.is_available:
            return item.payload
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "connector_mode": self.connector_mode,
            "items": {k: v.to_dict() for k, v in self.items.items()},
            "errors": [e.to_dict() for e in self.errors],
            "acquired_at": self.acquired_at.isoformat(),
        }


def mask_sensitive_credentials(text: str, token: str | None = None) -> str:
    """Helper to ensure tokens or secrets do not leak in error strings or outputs."""
    if not text:
        return text
    clean = text
    if isinstance(token, str) and token.strip():
        clean = clean.replace(token.strip(), "[MASKED_TOKEN]")
    import re

    pattern = (
        r"(?i)(bearer\s+|token[=:]\s*|password[=:]\s*|secret[=:]\s*|api[_-]?key[=:]\s*)"
        r"([^\s;&,#]+)"
    )
    clean = re.sub(pattern, r"\1[MASKED_SECRET]", clean)
    return clean


def sanitize_job_payload(payload: Any, token: str | None = None) -> Any:
    """Recursively sanitize sensitive credential fields in Databricks Job API payload."""
    if isinstance(payload, dict):
        sanitized: dict[str, Any] = {}
        for k, v in payload.items():
            lower_k = k.lower()
            if any(
                sens in lower_k
                for sens in ("password", "secret", "token", "api_key", "authorization")
            ):
                sanitized[k] = "[REDACTED_SECRET]"
            else:
                sanitized[k] = sanitize_job_payload(v, token)
        return sanitized
    elif isinstance(payload, list):
        return [sanitize_job_payload(item, token) for item in payload]
    elif isinstance(payload, str):
        return mask_sensitive_credentials(payload, token)
    return payload


class DatabricksEvidenceProvider:
    """Acquires normalized evidence from Databricks API using LiveDatabricksConnector."""

    def __init__(self, connector: DatabricksConnector | None = None) -> None:
        self.connector = connector or LiveDatabricksConnector()

    def acquire_pipeline_evidence(
        self,
        pipeline_id: str,
        job_id: str | int | None = None,
        cluster_id: str | None = None,
        policy_id: str | None = None,
        table_name: str | None = None,
        run_id: str | int | None = None,
        include_historical_runs: bool = False,
    ) -> NormalizedPipelineEvidence:
        """Acquire all requested evidence categories for a pipeline submission."""
        connector_mode = self.connector.mode()
        # Canonicalize live-api vs live mode string
        if connector_mode == "live-api":
            connector_mode = "live"

        evidence = NormalizedPipelineEvidence(
            pipeline_id=pipeline_id,
            connector_mode=connector_mode,
        )

        host = getattr(self.connector, "host", None)
        token = getattr(self.connector, "_token", None)

        # Helper method for safe acquisition of a single category
        def _acquire_category(
            cat: EvidenceCategory,
            fetch_func: Any,
            resource_id: str | None,
        ) -> None:
            prov = EvidenceProvenance(
                source_type="LIVE_API" if connector_mode in ("live", "live-api") else "FIXTURE",
                source_system="DATABRICKS",
                category=cat,
                workspace_host=host,
                resource_id=resource_id,
                is_mock=connector_mode == "offline-fixture",
            )
            try:
                res = fetch_func()
                if res is None:
                    # Object not found (404) or unavailable -> safe UNKNOWN representation
                    evidence.items[cat.value] = NormalizedEvidenceItem(
                        category=cat,
                        provenance=prov,
                        payload=None,
                        is_available=False,
                        error=AcquisitionError(
                            category=cat,
                            error_code=AcquisitionErrorCode.RESOURCE_NOT_FOUND,
                            message=f"{cat.value.capitalize()} resource not found: {resource_id}",
                            status_code=404,
                        ),
                    )
                else:
                    clean_res = sanitize_job_payload(res, token)
                    evidence.items[cat.value] = NormalizedEvidenceItem(
                        category=cat,
                        provenance=prov,
                        payload=clean_res,
                        is_available=True,
                    )
            except DatabricksApiError as e:
                err_code = self._map_status_code(e.status_code)
                clean_msg = mask_sensitive_credentials(str(e), token)
                acq_err = AcquisitionError(
                    category=cat,
                    error_code=err_code,
                    message=clean_msg,
                    status_code=e.status_code,
                )
                evidence.errors.append(acq_err)
                evidence.items[cat.value] = NormalizedEvidenceItem(
                    category=cat,
                    provenance=prov,
                    payload=None,
                    is_available=False,
                    error=acq_err,
                )
            except Exception as e:
                clean_msg = mask_sensitive_credentials(
                    f"Unexpected acquisition failure: {e}", token
                )
                acq_err = AcquisitionError(
                    category=cat,
                    error_code=AcquisitionErrorCode.UNKNOWN_ERROR,
                    message=clean_msg,
                    status_code=500,
                )
                evidence.errors.append(acq_err)
                evidence.items[cat.value] = NormalizedEvidenceItem(
                    category=cat,
                    provenance=prov,
                    payload=None,
                    is_available=False,
                    error=acq_err,
                )

        # Category 1: Workspace Status
        _acquire_category(
            EvidenceCategory.WORKSPACE,
            lambda: self.connector.get_workspace_status(),
            resource_id=host,
        )

        # Category 2: Pipeline Config
        if pipeline_id:
            _acquire_category(
                EvidenceCategory.PIPELINE,
                lambda: self.connector.get_pipeline(pipeline_id),
                resource_id=pipeline_id,
            )

        # Category 3: Job Config
        if job_id:
            _acquire_category(
                EvidenceCategory.JOB,
                lambda: self.connector.get_job(job_id),
                resource_id=str(job_id),
            )

        # Category 4: Cluster Config
        if cluster_id:
            _acquire_category(
                EvidenceCategory.CLUSTER,
                lambda: self.connector.get_cluster(cluster_id),
                resource_id=cluster_id,
            )

        # Category 5: Permissions
        if job_id:
            _acquire_category(
                EvidenceCategory.PERMISSIONS,
                lambda: self.connector.get_permissions("job", str(job_id)),
                resource_id=f"job/{job_id}",
            )
        elif pipeline_id:
            _acquire_category(
                EvidenceCategory.PERMISSIONS,
                lambda: self.connector.get_permissions("pipeline", pipeline_id),
                resource_id=f"pipeline/{pipeline_id}",
            )

        # Category 6: Table Profile
        if table_name:
            _acquire_category(
                EvidenceCategory.TABLE_PROFILE,
                lambda: self.connector.get_table_profile(table_name),
                resource_id=table_name,
            )

        # Category 7: Runtime Run Details
        if run_id:
            _acquire_category(
                EvidenceCategory.RUNTIME,
                lambda: self.connector.get_run(run_id),
                resource_id=str(run_id),
            )

        # Category 8: Historical Runs
        if job_id and include_historical_runs:
            _acquire_category(
                EvidenceCategory.HISTORICAL_RUNS,
                lambda: self.connector.get_recent_runs(job_id, limit=10),
                resource_id=str(job_id),
            )

        return evidence

    @staticmethod
    def _map_status_code(status_code: int | None) -> AcquisitionErrorCode:
        if status_code == 401:
            return AcquisitionErrorCode.AUTHENTICATION_FAILURE
        if status_code == 403:
            return AcquisitionErrorCode.AUTHORIZATION_FAILURE
        if status_code == 404:
            return AcquisitionErrorCode.RESOURCE_NOT_FOUND
        if status_code == 429:
            return AcquisitionErrorCode.RATE_LIMIT_EXCEEDED
        if status_code == 408:
            return AcquisitionErrorCode.TIMEOUT
        if status_code in (502, 503, 504):
            return AcquisitionErrorCode.API_UNAVAILABLE
        return AcquisitionErrorCode.UNKNOWN_ERROR
