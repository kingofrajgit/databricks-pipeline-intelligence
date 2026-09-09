"""Clean interface for current/future Databricks integration."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DatabricksConnector(ABC):
    """Abstract Databricks metadata provider.

    Phase 2 ships only the offline implementation. A future
    ``LiveDatabricksConnector`` can replace it without changing callers.
    """

    @abstractmethod
    def get_job(self, job_id: int | str) -> dict[str, Any] | None:
        """Return job configuration metadata (never secrets)."""

    @abstractmethod
    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        """Return cluster configuration metadata."""

    @abstractmethod
    def get_table_profile(self, table: str) -> dict[str, Any]:
        """Return table/data profile metadata."""

    @abstractmethod
    def get_recent_runs(self, job_id: int | str, limit: int = 10) -> list[dict[str, Any]]:
        """Return recent run metadata (empty when unavailable — never faked)."""

    def get_cluster_policy(self, policy_id: str) -> dict[str, Any] | None:
        """Return cluster policy definition if available."""
        return None

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        """Return pipeline/DLT configuration metadata if available."""
        return None

    def get_permissions(self, object_type: str, object_id: str) -> dict[str, Any] | None:
        """Return object permissions if available."""
        return None

    def get_workspace_status(self) -> dict[str, Any] | None:
        """Return workspace connectivity status if available."""
        return None

    def mode(self) -> str:
        return "abstract"
