"""Databricks Environment domain models (Phase 6).

Pydantic v2 models representing Databricks workspace entities:
Jobs, Tasks, Clusters, Policies, Pipelines, Schedules, and Permissions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dpif.models import CollectionMethod


class DatabricksWorkspace(BaseModel):
    """Metadata representing a Databricks workspace."""

    host: str = ""
    workspace_url: str = ""
    workspace_id: str | None = None
    cloud_provider: str = "unknown"
    region: str = "unknown"
    pricing_tier: str = "standard"
    status: str = "accessible"
    tags: dict[str, str] = Field(default_factory=dict)
    evidence_source: str = "DATABRICKS_API"
    collection_method: CollectionMethod = CollectionMethod.RUNTIME
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    def model_post_init(self, __context: Any) -> None:
        if not self.host and self.workspace_url:
            self.host = self.workspace_url
        elif not self.workspace_url and self.host:
            self.workspace_url = self.host


class TaskDependency(BaseModel):
    """Task dependency in a Databricks Job task graph."""

    task_key: str
    outcome: str | None = None


class DatabricksTask(BaseModel):
    """A task within a Databricks Job."""

    task_key: str
    description: str = ""
    depends_on: list[TaskDependency] = Field(default_factory=list)
    existing_cluster_id: str | None = None
    job_cluster_key: str | None = None
    new_cluster: dict[str, Any] | None = None
    notebook_task: dict[str, Any] | None = None
    spark_python_task: dict[str, Any] | None = None
    spark_jar_task: dict[str, Any] | None = None
    sql_task: dict[str, Any] | None = None
    pipeline_task: dict[str, Any] | None = None
    max_retries: int = 0
    min_retry_interval_millis: int = 0
    retry_on_timeout: bool = False
    timeout_seconds: int = 0
    libraries: list[dict[str, Any]] = Field(default_factory=list)

    def __init__(self, **data: Any) -> None:
        if "notebook_path" in data and not data.get("notebook_task"):
            data["notebook_task"] = {"notebook_path": data.pop("notebook_path")}
        if "spark_python_task_file" in data and not data.get("spark_python_task"):
            data["spark_python_task"] = {"python_file": data.pop("spark_python_task_file")}
        super().__init__(**data)

    @property
    def source_path(self) -> str | None:
        """Extract the primary executable source path if available."""
        if self.notebook_task:
            return self.notebook_task.get("notebook_path")
        if self.spark_python_task:
            return self.spark_python_task.get("python_file")
        if self.sql_task:
            query = self.sql_task.get("query", {})
            return query.get("query_id") or self.sql_task.get("file", {}).get("path")
        if self.pipeline_task:
            return self.pipeline_task.get("pipeline_id")
        return None

    @property
    def task_type(self) -> str:
        """Classify task type."""
        if self.notebook_task:
            return "notebook"
        if self.spark_python_task:
            return "spark_python"
        if self.spark_jar_task:
            return "spark_jar"
        if self.sql_task:
            return "sql"
        if self.pipeline_task:
            return "pipeline"
        return "unknown"


class JobSchedule(BaseModel):
    """Job schedule definition."""

    quartz_cron_expression: str = ""
    timezone_id: str = "UTC"
    pause_status: str = "UNPAUSED"


class DatabricksJob(BaseModel):
    """A Databricks workflow/job definition."""

    job_id: int | str
    name: str = ""
    creator_user_name: str = ""
    tasks: list[DatabricksTask] = Field(default_factory=list)
    schedule: JobSchedule | None = None
    max_concurrent_runs: int = 1
    max_retries: int = 0
    timeout_seconds: int = 0
    job_clusters: list[dict[str, Any]] = Field(default_factory=list)
    git_source: dict[str, Any] | None = None
    parameters: list[dict[str, Any]] = Field(default_factory=list)
    evidence_source: str = "DATABRICKS_API"
    collection_method: CollectionMethod = CollectionMethod.RUNTIME
    retrieval_timestamp: datetime = Field(default_factory=datetime.utcnow)

    @property
    def is_paused(self) -> bool:
        if self.schedule:
            return self.schedule.pause_status.upper() == "PAUSED"
        return False

    @property
    def task_keys(self) -> set[str]:
        return {t.task_key for t in self.tasks}

    @property
    def total_retries(self) -> int:
        """Max retries across all tasks."""
        if not self.tasks:
            return self.max_retries
        return max(t.max_retries for t in self.tasks)


class AutoscalingConfig(BaseModel):
    """Cluster autoscaling specification."""

    min_workers: int
    max_workers: int


class DatabricksCluster(BaseModel):
    """A Databricks compute cluster configuration."""

    cluster_id: str
    cluster_name: str = ""
    spark_version: str = ""
    node_type_id: str = ""
    driver_node_type_id: str | None = None
    num_workers: int | None = None
    autoscale: AutoscalingConfig | None = None
    photon: bool = False
    runtime_engine: str = "STANDARD"
    policy_id: str | None = None
    autotermination_minutes: int = 0
    spark_conf: dict[str, str] = Field(default_factory=dict)
    spark_env_vars: dict[str, str] = Field(default_factory=dict)
    custom_tags: dict[str, str] = Field(default_factory=dict)
    cluster_source: str = "JOB"  # JOB or UI or API
    state: str = "RUNNING"
    evidence_source: str = "DATABRICKS_API"
    collection_method: CollectionMethod = CollectionMethod.RUNTIME
    retrieval_timestamp: datetime = Field(default_factory=datetime.utcnow)

    @property
    def is_autoscaling(self) -> bool:
        return self.autoscale is not None

    @property
    def has_autoscaling(self) -> bool:
        return self.is_autoscaling

    @property
    def is_photon(self) -> bool:
        return self.photon or self.runtime_engine.upper() == "PHOTON"

    @property
    def effective_min_workers(self) -> int:
        if self.autoscale:
            return self.autoscale.min_workers
        return self.num_workers or 0

    @property
    def effective_max_workers(self) -> int:
        if self.autoscale:
            return self.autoscale.max_workers
        return self.num_workers or 0

    @property
    def effective_workers(self) -> int:
        return self.effective_max_workers

    @property
    def dbr_major_version(self) -> str:
        ver = self.spark_version.split(".x")[0]
        parts = ver.split(".")
        return ".".join(parts[:2]) if len(parts) >= 2 else ver


class ClusterPolicy(BaseModel):
    """Databricks Cluster Policy definition."""

    policy_id: str
    name: str = ""
    definition: dict[str, Any] = Field(default_factory=dict)
    description: str = ""


class DatabricksPipeline(BaseModel):
    """Databricks Delta Live Tables (DLT) / Lakeflow pipeline."""

    pipeline_id: str
    name: str = ""
    target: str = ""
    continuous: bool = False
    development: bool = False
    photon: bool = False
    clusters: list[dict[str, Any]] = Field(default_factory=list)
    libraries: list[dict[str, Any]] = Field(default_factory=list)
    configuration: dict[str, Any] = Field(default_factory=dict)
    edition: str = "ADVANCED"
    channel: str = "CURRENT"
    evidence_source: str = "DATABRICKS_API"
    collection_method: CollectionMethod = CollectionMethod.RUNTIME


class AccessControlEntry(BaseModel):
    """Individual access control entry."""

    user_name: str | None = None
    group_name: str | None = None
    service_principal_name: str | None = None
    permission_level: str  # CAN_MANAGE, CAN_RUN, CAN_VIEW, etc.


class PermissionConfiguration(BaseModel):
    """Access control list for a Databricks object."""

    object_id: str
    object_type: str  # job, cluster, pipeline, etc.
    access_control_list: list[AccessControlEntry] = Field(default_factory=list)
    evidence_source: str = "DATABRICKS_API"
