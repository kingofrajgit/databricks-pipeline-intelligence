"""Live Databricks REST API connector (Phase 6).

Interacts with the Databricks REST APIs (2.0/2.1) using requests.
Handles authentication, rate limits, timeouts, and error responses gracefully.
Never logs or leaks credentials.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from dpif.connectors.base import DatabricksConnector

logger = logging.getLogger(__name__)


class DatabricksApiError(Exception):
    """Exception raised for Databricks API failures with status code."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


DatabricksAPIError = DatabricksApiError


class LiveDatabricksConnector(DatabricksConnector):
    """Live connector for Databricks workspaces via REST API."""

    def __init__(
        self,
        host: str | None = None,
        token: str | None = None,
        timeout_seconds: float = 15.0,
    ) -> None:
        self.host = (host or os.environ.get("DATABRICKS_HOST", "")).strip().rstrip("/")
        if self.host and not self.host.startswith(("http://", "https://")):
            self.host = f"https://{self.host}"
        self._token = (token or os.environ.get("DATABRICKS_TOKEN", "")).strip()
        self.timeout = timeout_seconds

    @property
    def is_configured(self) -> bool:
        """True if host and token are provided."""
        return bool(self.host and self._token)

    @property
    def headers(self) -> dict[str, str]:
        return self._headers()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "Databricks-Pipeline-Intelligence-Framework/0.1.0",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Execute request safely without exposing token."""
        if not self.is_configured:
            logger.warning("Databricks credentials not configured (host or token missing)")
            raise DatabricksApiError("Databricks credentials not configured", status_code=401)

        url = f"{self.host}/{endpoint.lstrip('/')}"
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=self._headers(),
                params=params,
                json=json_data,
                timeout=self.timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    return {**data, "_connector": "live-api"}
                return {"result": data, "_connector": "live-api"}
            elif resp.status_code == 404:
                logger.info("Databricks object not found at %s: 404", endpoint)
                return None
            elif resp.status_code in (401, 403):
                logger.warning(
                    "Databricks authentication/permission failure at %s: %s",
                    endpoint,
                    resp.status_code,
                )
                clean_text = (
                    resp.text.replace(self._token, "[MASKED]") if self._token else resp.text
                )
                raise DatabricksApiError(
                    f"Authentication/Permission failure ({resp.status_code}): {clean_text}",
                    status_code=resp.status_code,
                )
            elif resp.status_code == 429:
                logger.warning("Databricks API rate limit exceeded at %s: 429", endpoint)
                raise DatabricksApiError(
                    "Databricks API rate limit exceeded (429)", status_code=429
                )
            else:
                logger.warning("Databricks API error %s at %s", resp.status_code, endpoint)
                raise DatabricksApiError(
                    f"Databricks API error {resp.status_code}", status_code=resp.status_code
                )
        except requests.exceptions.Timeout as e:
            logger.warning(
                "Databricks request timed out after %s seconds for %s", self.timeout, endpoint
            )
            raise DatabricksApiError(
                f"Databricks request timed out after {self.timeout}s: {endpoint}", status_code=408
            ) from e
        except requests.exceptions.RequestException as e:
            logger.warning("Databricks network error: %s", type(e).__name__)
            raise DatabricksApiError(
                f"Databricks network error: {type(e).__name__}", status_code=503
            ) from e

    def get_job(self, job_id: int | str) -> dict[str, Any] | None:
        """Fetch job configuration via /api/2.1/jobs/get."""
        return self._request("GET", "/api/2.1/jobs/get", params={"job_id": job_id})

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        """Fetch cluster configuration via /api/2.0/clusters/get."""
        return self._request("GET", "/api/2.0/clusters/get", params={"cluster_id": cluster_id})

    def get_cluster_policy(self, policy_id: str) -> dict[str, Any] | None:
        """Fetch cluster policy via /api/2.0/cluster-policies/get."""
        return self._request(
            "GET", "/api/2.0/cluster-policies/get", params={"policy_id": policy_id}
        )

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        """Fetch pipeline (DLT) configuration via /api/2.0/pipelines/{pipeline_id}."""
        return self._request("GET", f"/api/2.0/pipelines/{pipeline_id}")

    def get_permissions(self, object_type: str, object_id: str) -> dict[str, Any] | None:
        """Fetch permissions via /api/2.0/permissions/{object_type}s/{object_id}."""
        plural = f"{object_type.rstrip('s')}s"
        return self._request("GET", f"/api/2.0/permissions/{plural}/{object_id}")

    def get_table_profile(self, table: str) -> dict[str, Any]:
        """Live table profile requires UC or runtime scan; returns empty when not scanned."""
        return {"table": table, "status": "runtime-scan-required", "_connector": "live-api"}

    def get_recent_runs(self, job_id: int | str, limit: int = 10) -> list[dict[str, Any]]:
        """Fetch recent runs for a job via /api/2.1/jobs/runs/list."""
        res = self._request(
            "GET",
            "/api/2.1/jobs/runs/list",
            params={"job_id": job_id, "limit": limit, "active_only": "false"},
        )
        if not res:
            return []
        runs = res.get("runs", [])
        return runs if isinstance(runs, list) else []

    def get_run(self, run_id: int | str) -> dict[str, Any] | None:
        """Fetch job run details via /api/2.1/jobs/runs/get."""
        return self._request("GET", "/api/2.1/jobs/runs/get", params={"run_id": run_id})

    def get_workspace_status(self) -> dict[str, Any] | None:
        """Check workspace connectivity via /api/2.0/clusters/spark-versions."""
        res = self._request("GET", "/api/2.0/clusters/spark-versions")
        if not res or res.get("error"):
            return None
        versions = res.get("versions", [])
        count = len(versions) if isinstance(versions, list) else 0
        return {
            "host": self.host,
            "status": "connected",
            "spark_versions_count": count,
            "_connector": "live-api",
        }

    def mode(self) -> str:
        return "live-api"
