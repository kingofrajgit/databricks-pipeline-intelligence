"""Runtime Performance Intelligence Domain Models (Phase 7).

Strongly typed Pydantic v2 models representing real execution evidence
from Databricks Jobs API, Spark execution metrics, event logs, and offline fixtures.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dpif.models import CollectionMethod


class RuntimeTaskMetrics(BaseModel):
    """Task-level execution metrics from Spark task metrics."""

    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spill_bytes: int = 0
    disk_spill_bytes: int = 0
    executor_run_time_ms: int = 0
    executor_cpu_time_ms: int = 0
    jvm_gc_time_ms: int = 0
    result_serialization_time_ms: int = 0


class RuntimeTask(BaseModel):
    """Execution details for an individual Spark task."""

    task_id: int | str
    stage_id: int
    attempt: int = 0
    status: str = "SUCCESS"
    duration_seconds: float = 0.0
    metrics: RuntimeTaskMetrics | None = None
    failure_reason: str | None = None
    executor_id: str | None = None
    host: str | None = None
    evidence_source: str = "DATABRICKS_API"


class RuntimeStage(BaseModel):
    """Execution details and aggregated metrics for a Spark stage."""

    stage_id: int
    name: str = ""
    status: str = "COMPLETE"
    task_count: int = 0
    failed_task_count: int = 0
    duration_seconds: float = 0.0
    input_bytes: int | None = None
    output_bytes: int | None = None
    shuffle_read_bytes: int | None = None
    shuffle_write_bytes: int | None = None
    memory_spill_bytes: int | None = None
    disk_spill_bytes: int | None = None
    jvm_gc_time_ms: int | None = None
    executor_run_time_ms: int | None = None
    tasks: list[RuntimeTask] = Field(default_factory=list)
    evidence_source: str = "DATABRICKS_API"

    @property
    def total_shuffle_bytes(self) -> int:
        """Total shuffle bytes (read + write)."""
        sr = self.shuffle_read_bytes
        sw = self.shuffle_write_bytes
        if sr is not None or sw is not None:
            return (sr or 0) + (sw or 0)
        if self.tasks:
            return sum(
                (
                    (t.metrics.shuffle_read_bytes if t.metrics else 0)
                    + (t.metrics.shuffle_write_bytes if t.metrics else 0)
                )
                for t in self.tasks
            )
        return 0

    @property
    def shuffle_to_input_ratio(self) -> float | None:
        """Ratio of total shuffle bytes to input bytes. None if input unavailable or zero."""
        if self.input_bytes is None or self.input_bytes <= 0:
            return None
        return self.total_shuffle_bytes / float(self.input_bytes)

    @property
    def gc_ratio(self) -> float | None:
        """Ratio of JVM GC time to executor runtime. None if executor time unavailable or zero."""
        gc = self.jvm_gc_time_ms
        exec_time = self.executor_run_time_ms
        if gc is None and self.tasks:
            gc = sum(t.metrics.jvm_gc_time_ms for t in self.tasks if t.metrics)
        if exec_time is None and self.tasks:
            exec_time = sum(t.metrics.executor_run_time_ms for t in self.tasks if t.metrics)
        if exec_time is None or exec_time <= 0 or gc is None:
            return None
        return float(gc) / float(exec_time)

    @property
    def task_durations(self) -> list[float]:
        """List of task execution durations in seconds."""
        return [t.duration_seconds for t in self.tasks if t.duration_seconds is not None]


class RuntimeRun(BaseModel):
    """Complete runtime execution evidence for a Databricks Job run or pipeline execution."""

    run_id: int | str
    job_id: int | str | None = None
    run_name: str = ""
    status: str = "SUCCESS"
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_seconds: float = 0.0
    stages: list[RuntimeStage] = Field(default_factory=list)
    executor_failures: list[dict[str, Any]] = Field(default_factory=list)
    cluster_utilization: dict[str, Any] | float | int | None = None
    evidence_source: str = "DATABRICKS_API"
    collection_method: CollectionMethod = CollectionMethod.RUNTIME

    @property
    def execution_duration_ms(self) -> float:
        return self.duration_seconds * 1000.0

    @property
    def gc_ratio(self) -> float:
        total_gc = sum(s.jvm_gc_time_ms or 0 for s in self.stages)
        total_exec = sum(s.executor_run_time_ms or 0 for s in self.stages)
        if total_exec <= 0:
            return 0.0
        return float(total_gc) / float(total_exec)

    @property
    def total_input_bytes(self) -> int:
        return sum(s.input_bytes or 0 for s in self.stages)

    @property
    def total_output_bytes(self) -> int:
        return sum(s.output_bytes or 0 for s in self.stages)

    @property
    def total_shuffle_bytes(self) -> int:
        return sum(s.total_shuffle_bytes for s in self.stages)

    @property
    def total_memory_spill_bytes(self) -> int:
        return sum(s.memory_spill_bytes or 0 for s in self.stages)

    @property
    def total_disk_spill_bytes(self) -> int:
        return sum(s.disk_spill_bytes or 0 for s in self.stages)

    @property
    def total_spill_bytes(self) -> int:
        return self.total_memory_spill_bytes + self.total_disk_spill_bytes

    @property
    def total_failed_tasks(self) -> int:
        return sum(s.failed_task_count for s in self.stages)

    @property
    def total_tasks(self) -> int:
        return sum(s.task_count for s in self.stages)


class CorrelationResult(BaseModel):
    """Result of cross-domain static risk to runtime evidence correlation."""

    static_rule_id: str
    runtime_rule_ids: list[str] = Field(default_factory=list)
    status: str  # STATIC_RISK_CONFIRMED, RUNTIME_SUPPORTS_STATIC_RISK, etc.
    description: str = ""
    confidence: float = 0.8
    evidence: list[str] = Field(default_factory=list)
