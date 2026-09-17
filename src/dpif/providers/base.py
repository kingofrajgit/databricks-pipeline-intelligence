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

    # Mask explicitly passed token
    if isinstance(token, str) and token.strip():
        clean = clean.replace(token.strip(), "[MASKED_TOKEN]")

    # Also mask ambient tokens from environment if set
    import os

    for env_key in ("DATABRICKS_TOKEN", "DPIF_DATABRICKS_TOKEN"):
        env_token = os.environ.get(env_key)
        if env_token and env_token.strip():
            clean = clean.replace(env_token.strip(), "[MASKED_TOKEN]")

    import re

    # Mask bearer tokens
    bearer_pattern = r"(?i)(bearer\s+)([^\s;&,#\"']+)"
    clean = re.sub(bearer_pattern, r"\g<1>[MASKED_SECRET]", clean)

    # Mask authorization, password, token, secret, api_key key-value pairs
    # Excludes "bearer" via negative lookahead so Bearer is preserved when present
    pattern = (
        r'(?i)("?(?:authorization|password|token|secret|api[_-]?key)"?\s*[:=]\s*)'
        r'(?!"?bearer\b)("?[^\s;&,#"\']+"?)'
    )
    clean = re.sub(pattern, r"\g<1>[MASKED_SECRET]", clean)

    return clean


def sanitize_job_payload(payload: Any, token: str | None = None) -> Any:
    """Recursively sanitize sensitive credential fields in Databricks Job API payload."""
    if isinstance(payload, dict):
        sanitized: dict[str, Any] = {}
        for k, v in payload.items():
            lower_k = k.lower()
            if any(
                sens in lower_k
                for sens in (
                    "password",
                    "secret",
                    "token",
                    "api_key",
                    "authorization",
                    "bearer",
                )
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
                    if cat == EvidenceCategory.CLUSTER:
                        valid_cluster = (
                            isinstance(res, dict)
                            and isinstance(res.get("cluster_id"), str)
                            and bool(res["cluster_id"].strip())
                        )
                        if not valid_cluster:
                            raise DatabricksApiError(
                                f"Malformed response: missing valid cluster_id ({resource_id})",
                                status_code=200,
                            )
                    elif cat == EvidenceCategory.RUNTIME:
                        has_str_id = isinstance(res.get("run_id"), str) and bool(
                            res["run_id"].strip()
                        )
                        has_int_id = isinstance(res.get("run_id"), int) and res["run_id"] > 0
                        valid_run_id = isinstance(res, dict) and (has_str_id or has_int_id)
                        if not valid_run_id:
                            raise DatabricksApiError(
                                f"Malformed response: missing valid run_id ({resource_id})",
                                status_code=200,
                            )
                    elif cat == EvidenceCategory.HISTORICAL_RUNS:
                        if isinstance(res, dict) and isinstance(res.get("runs"), list):
                            hist_runs = res["runs"]
                        elif isinstance(res, dict) and isinstance(res.get("result"), list):
                            hist_runs = res["result"]
                        elif (
                            isinstance(res, dict)
                            and res.get("runs") is None
                            and isinstance(res.get("has_more"), bool)
                        ):
                            hist_runs = []
                        elif isinstance(res, list):
                            hist_runs = res
                        else:
                            raise DatabricksApiError(
                                f"Malformed response: invalid historical shape ({resource_id})",
                                status_code=200,
                            )
                        clean_res = sanitize_job_payload(hist_runs, token)
                        evidence.items[cat.value] = NormalizedEvidenceItem(
                            category=cat,
                            provenance=prov,
                            payload=clean_res,
                            is_available=True,
                        )
                        return
                    clean_res = sanitize_job_payload(res, token)
                    evidence.items[cat.value] = NormalizedEvidenceItem(
                        category=cat,
                        provenance=prov,
                        payload=clean_res,
                        is_available=True,
                    )
            except DatabricksApiError as e:
                err_code = self._map_status_code(e.status_code, str(e))
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

        # Cross-reference cluster_id from Job evidence if cluster_id not explicitly provided
        effective_cluster_id = cluster_id
        if not effective_cluster_id and EvidenceCategory.JOB.value in evidence.items:
            job_item = evidence.items[EvidenceCategory.JOB.value]
            if job_item.is_available and isinstance(job_item.payload, dict):
                settings = job_item.payload.get("settings", {})
                tasks = settings.get("tasks", []) if isinstance(settings, dict) else []
                for task in tasks:
                    if isinstance(task, dict) and "existing_cluster_id" in task:
                        effective_cluster_id = str(task["existing_cluster_id"])
                        break

        # Category 4: Cluster Config
        if effective_cluster_id:
            _acquire_category(
                EvidenceCategory.CLUSTER,
                lambda: self.connector.get_cluster(effective_cluster_id),
                resource_id=effective_cluster_id,
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

        # Category 9: Code Discovery
        def _acquire_code() -> dict[str, Any] | None:
            import base64

            tasks: list[dict[str, Any]] = []
            job_item = evidence.items.get(EvidenceCategory.JOB.value)
            if job_item and job_item.is_available and isinstance(job_item.payload, dict):
                settings = job_item.payload.get("settings", job_item.payload)
                if isinstance(settings, dict):
                    raw_tasks = settings.get("tasks", [])
                    if isinstance(raw_tasks, list):
                        tasks = [t for t in raw_tasks if isinstance(t, dict)]

            # Check if any job tasks reference a pipeline_task
            pipeline_tasks_to_add: list[dict[str, Any]] = []
            for t in tasks:
                if "pipeline_task" in t and isinstance(t["pipeline_task"], dict):
                    sub_pid = t["pipeline_task"].get("pipeline_id")
                    if sub_pid:
                        try:
                            sub_pipe = self.connector.get_pipeline(sub_pid)
                            if sub_pipe and isinstance(sub_pipe, dict):
                                spec = (
                                    sub_pipe.get("spec", sub_pipe)
                                    if isinstance(sub_pipe.get("spec"), dict)
                                    else sub_pipe
                                )
                                libs = spec.get("libraries", [])
                                if isinstance(libs, list):
                                    for i, lib in enumerate(libs):
                                        if isinstance(lib, dict):
                                            if "notebook" in lib and isinstance(lib["notebook"], dict):
                                                pipeline_tasks_to_add.append({
                                                    "task_key": f"{t.get('task_key', 'task')}_nb_{i+1}",
                                                    "notebook_task": {"notebook_path": lib["notebook"].get("path")},
                                                })
                                            elif "file" in lib and isinstance(lib["file"], dict):
                                                pipeline_tasks_to_add.append({
                                                    "task_key": f"{t.get('task_key', 'task')}_file_{i+1}",
                                                    "spark_python_task": {"python_file": lib["file"].get("path")},
                                                })
                        except Exception as e:
                            logger.info("Could not fetch sub-pipeline %s for code discovery: %s", sub_pid, e)

            tasks.extend(pipeline_tasks_to_add)

            if not tasks:
                pipe_item = evidence.items.get(EvidenceCategory.PIPELINE.value)
                if pipe_item and pipe_item.is_available and isinstance(pipe_item.payload, dict):
                    spec = (
                        pipe_item.payload.get("spec", pipe_item.payload)
                        if isinstance(pipe_item.payload.get("spec"), dict)
                        else pipe_item.payload
                    )
                    libs = spec.get("libraries", [])
                    if isinstance(libs, list):
                        for i, lib in enumerate(libs):
                            if isinstance(lib, dict):
                                if "notebook" in lib and isinstance(lib["notebook"], dict):
                                    tasks.append({
                                        "task_key": f"pipeline_notebook_{i+1}",
                                        "notebook_task": {"notebook_path": lib["notebook"].get("path")},
                                    })
                                elif "file" in lib and isinstance(lib["file"], dict):
                                    tasks.append({
                                        "task_key": f"pipeline_file_{i+1}",
                                        "spark_python_task": {"python_file": lib["file"].get("path")},
                                    })

            if not tasks:
                return None

            discovered_tasks: list[dict[str, Any]] = []

            for task in tasks:
                task_key = str(task.get("task_key", "task"))
                # Notebook task
                if "notebook_task" in task and isinstance(task["notebook_task"], dict):
                    nb_path = task["notebook_task"].get("notebook_path")
                    if nb_path:
                        try:
                            res = self.connector.export_workspace_object(nb_path, format="SOURCE")
                            if res and isinstance(res, dict) and "content" in res:
                                try:
                                    raw = base64.b64decode(res["content"]).decode("utf-8", errors="replace")
                                except Exception:
                                    raw = str(res["content"])
                                file_type = str(res.get("file_type", "PYTHON")).lower()
                                lang = "sql" if file_type == "sql" else "python"
                                discovered_tasks.append({
                                    "task_key": task_key,
                                    "task_type": "notebook",
                                    "path": nb_path,
                                    "source_code": raw,
                                    "language": lang,
                                })
                        except DatabricksApiError as e:
                            if e.status_code in (401, 403):
                                raise
                # Spark Python task
                elif "spark_python_task" in task and isinstance(task["spark_python_task"], dict):
                    py_file = task["spark_python_task"].get("python_file")
                    if py_file:
                        try:
                            code_str: str | None = None
                            if py_file.startswith("dbfs:") or py_file.startswith("/dbfs/"):
                                res = self.connector.read_dbfs_file(py_file)
                                if res and isinstance(res, dict) and "data" in res:
                                    try:
                                        code_str = base64.b64decode(res["data"]).decode("utf-8", errors="replace")
                                    except Exception:
                                        code_str = str(res["data"])
                            if code_str is None:
                                res = self.connector.export_workspace_object(py_file, format="AUTO")
                                if res and isinstance(res, dict) and "content" in res:
                                    try:
                                        code_str = base64.b64decode(res["content"]).decode("utf-8", errors="replace")
                                    except Exception:
                                        code_str = str(res["content"])
                            if code_str is not None:
                                discovered_tasks.append({
                                    "task_key": task_key,
                                    "task_type": "spark_python",
                                    "path": py_file,
                                    "source_code": code_str,
                                    "language": "python",
                                })
                        except DatabricksApiError as e:
                            if e.status_code in (401, 403):
                                raise
                # SQL task
                elif "sql_task" in task and isinstance(task["sql_task"], dict):
                    sql_task = task["sql_task"]
                    query_id = (
                        sql_task.get("query", {}).get("query_id")
                        if isinstance(sql_task.get("query"), dict)
                        else None
                    )
                    file_path = (
                        sql_task.get("file", {}).get("path")
                        if isinstance(sql_task.get("file"), dict)
                        else None
                    )
                    if query_id:
                        try:
                            res = self.connector.get_sql_query(query_id)
                            if res and isinstance(res, dict):
                                sql_text = res.get("query_text") or res.get("query") or res.get("sql") or ""
                                if sql_text:
                                    discovered_tasks.append({
                                        "task_key": task_key,
                                        "task_type": "sql",
                                        "path": f"query://{query_id}",
                                        "source_code": sql_text,
                                        "language": "sql",
                                    })
                        except DatabricksApiError as e:
                            if e.status_code in (401, 403):
                                raise
                    elif file_path:
                        try:
                            res = self.connector.export_workspace_object(file_path, format="SOURCE")
                            if res and isinstance(res, dict) and "content" in res:
                                try:
                                    raw = base64.b64decode(res["content"]).decode("utf-8", errors="replace")
                                except Exception:
                                    raw = str(res["content"])
                                discovered_tasks.append({
                                    "task_key": task_key,
                                    "task_type": "sql",
                                    "path": file_path,
                                    "source_code": raw,
                                    "language": "sql",
                                })
                        except DatabricksApiError as e:
                            if e.status_code in (401, 403):
                                raise

            if not discovered_tasks:
                return None

            primary_task = discovered_tasks[0]
            combined_code = "\n\n".join(
                f"# --- TASK: {t['task_key']} ({t['path']}) ---\n{t['source_code']}"
                for t in discovered_tasks
            )
            return {
                "tasks": discovered_tasks,
                "primary_task_key": primary_task["task_key"],
                "primary_source_code": primary_task["source_code"],
                "primary_filename": f"{primary_task['task_key']}.{'sql' if primary_task['language'] == 'sql' else 'py'}",
                "combined_code": combined_code,
            }

        _acquire_category(
            EvidenceCategory.CODE,
            _acquire_code,
            resource_id=f"job/{job_id}/code" if job_id else f"pipeline/{pipeline_id}/code",
        )

        return evidence

    @staticmethod
    def _map_status_code(status_code: int | None, message: str = "") -> AcquisitionErrorCode:
        if "malformed json" in message.lower() or status_code == 200:
            return AcquisitionErrorCode.MALFORMED_RESPONSE
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
