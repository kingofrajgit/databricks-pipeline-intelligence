"""Metric normalization layer for runtime performance intelligence (Phase 7).

Normalizes units (ms -> seconds, bytes -> GB), computes distribution statistics,
and deserializes raw payload dictionaries into validated RuntimeRun objects.
"""

from __future__ import annotations

import math
from typing import Any

from dpif.models import CollectionMethod
from dpif.runtime.models import (
    RuntimeRun,
    RuntimeStage,
    RuntimeTask,
    RuntimeTaskMetrics,
)


def bytes_to_gb(bytes_val: int | float | None) -> float | None:
    """Convert bytes to gigabytes (1 GB = 1024^3 bytes). Returns None if input is None."""
    if bytes_val is None:
        return None
    return float(bytes_val) / (1024.0**3)


def ms_to_seconds(ms_val: int | float | None) -> float | None:
    """Convert milliseconds to seconds. Returns None if input is None."""
    if ms_val is None:
        return None
    return float(ms_val) / 1000.0


def calculate_distribution(values: list[float]) -> dict[str, float]:
    """Calculate statistical distribution metrics: min, median, p95, p99, max, avg.

    Handles empty or single-item lists safely.
    """
    if not values:
        return {
            "min": 0.0,
            "median": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
            "avg": 0.0,
            "count": 0.0,
        }

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def _percentile(p: float) -> float:
        if n == 1:
            return sorted_vals[0]
        k = (n - 1) * p
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        d0 = sorted_vals[int(f)] * (c - k)
        d1 = sorted_vals[int(c)] * (k - f)
        return d0 + d1

    return {
        "min": float(sorted_vals[0]),
        "median": _percentile(0.50),
        "p95": _percentile(0.95),
        "p99": _percentile(0.99),
        "max": float(sorted_vals[-1]),
        "avg": float(sum(sorted_vals)) / float(n),
        "count": float(n),
    }


def normalize_runtime_payload(
    raw: dict[str, Any], default_source: str = "DATABRICKS_API"
) -> RuntimeRun:
    """Normalize a raw dictionary into a strongly-typed RuntimeRun model.

    Handles raw Databricks Jobs API responses, Spark UI REST payloads,
    and offline fixture JSON representations with robust error handling.
    """
    evidence_source = str(raw.get("evidence_source", default_source))
    col_method_raw = raw.get("collection_method")
    if col_method_raw:
        try:
            col_method = CollectionMethod(str(col_method_raw).lower())
        except Exception:
            col_method = (
                CollectionMethod.FIXTURE
                if evidence_source == "FIXTURE"
                else CollectionMethod.RUNTIME
            )
    else:
        col_method = (
            CollectionMethod.FIXTURE if evidence_source == "FIXTURE" else CollectionMethod.RUNTIME
        )

    duration_sec = float(
        raw.get("duration_seconds", 0.0)
        or (float(raw.get("execution_duration_ms", 0.0) or 0.0) / 1000.0)
        or (float(raw.get("execution_duration", 0.0) or 0.0) / 1000.0)
        or (float(raw.get("duration_ms", 0.0) or 0.0) / 1000.0)
    )

    stages_raw = raw.get("stages", [])
    stages: list[RuntimeStage] = []

    for s in stages_raw:
        if isinstance(s, RuntimeStage):
            stages.append(s)
            continue
        if not isinstance(s, dict):
            continue

        tasks_raw = s.get("tasks", [])
        tasks: list[RuntimeTask] = []
        if not tasks_raw and (s.get("task_durations") or s.get("task_input_bytes")):
            t_durs = s.get("task_durations", [])
            t_inputs = s.get("task_input_bytes", [])
            max_len = max(len(t_durs), len(t_inputs))
            for idx in range(max_len):
                d_val = float(t_durs[idx]) if idx < len(t_durs) else 0.0
                d_sec = d_val / 1000.0 if d_val > 100 else d_val
                in_b = int(t_inputs[idx]) if idx < len(t_inputs) else 0
                tasks.append(
                    RuntimeTask(
                        task_id=idx + 1,
                        stage_id=int(s.get("stage_id", 0) or 0),
                        duration_seconds=d_sec,
                        metrics=RuntimeTaskMetrics(input_bytes=in_b),
                        evidence_source=evidence_source,
                    )
                )
        for t in tasks_raw:
            if isinstance(t, RuntimeTask):
                tasks.append(t)
                continue
            if not isinstance(t, dict):
                continue
            m_raw = t.get("metrics")
            metrics = None
            if isinstance(m_raw, RuntimeTaskMetrics):
                metrics = m_raw
            elif isinstance(m_raw, dict):
                metrics = RuntimeTaskMetrics(
                    input_bytes=int(m_raw.get("input_bytes", 0) or 0),
                    output_bytes=int(m_raw.get("output_bytes", 0) or 0),
                    shuffle_read_bytes=int(m_raw.get("shuffle_read_bytes", 0) or 0),
                    shuffle_write_bytes=int(m_raw.get("shuffle_write_bytes", 0) or 0),
                    memory_spill_bytes=int(m_raw.get("memory_spill_bytes", 0) or 0),
                    disk_spill_bytes=int(m_raw.get("disk_spill_bytes", 0) or 0),
                    executor_run_time_ms=int(m_raw.get("executor_run_time_ms", 0) or 0),
                    executor_cpu_time_ms=int(m_raw.get("executor_cpu_time_ms", 0) or 0),
                    jvm_gc_time_ms=int(m_raw.get("jvm_gc_time_ms", 0) or 0),
                    result_serialization_time_ms=int(
                        m_raw.get("result_serialization_time_ms", 0) or 0
                    ),
                )
            t_dur = float(
                t.get("duration_seconds", 0.0) or (float(t.get("duration_ms", 0.0) or 0.0) / 1000.0)
            )
            tasks.append(
                RuntimeTask(
                    task_id=t.get("task_id", 0),
                    stage_id=int(t.get("stage_id", s.get("stage_id", 0)) or 0),
                    attempt=int(t.get("attempt", 0) or 0),
                    status=str(t.get("status", "SUCCESS")),
                    duration_seconds=t_dur,
                    metrics=metrics,
                    failure_reason=t.get("failure_reason"),
                    executor_id=str(t.get("executor_id")) if t.get("executor_id") else None,
                    host=str(t.get("host")) if t.get("host") else None,
                    evidence_source=evidence_source,
                )
            )

        s_dur = float(
            s.get("duration_seconds", 0.0) or (float(s.get("duration_ms", 0.0) or 0.0) / 1000.0)
        )
        task_count = int(s.get("task_count", len(tasks)) or len(tasks))
        failed_count = int(
            s.get(
                "failed_task_count",
                sum(1 for task in tasks if task.status.upper() in ("FAILED", "KILLED")),
            )
            or 0
        )

        stages.append(
            RuntimeStage(
                stage_id=int(s.get("stage_id", 0) or 0),
                name=str(s.get("name", f"Stage {s.get('stage_id', 0)}")),
                status=str(s.get("status", "COMPLETE")),
                task_count=task_count,
                failed_task_count=failed_count,
                duration_seconds=s_dur,
                input_bytes=s.get("input_bytes"),
                output_bytes=s.get("output_bytes"),
                shuffle_read_bytes=s.get("shuffle_read_bytes"),
                shuffle_write_bytes=s.get("shuffle_write_bytes"),
                memory_spill_bytes=(
                    s.get("memory_spill_bytes")
                    if s.get("memory_spill_bytes") is not None
                    else (s.get("memory_spilled_bytes") or s.get("memoryBytesSpilled"))
                ),
                disk_spill_bytes=(
                    s.get("disk_spill_bytes")
                    if s.get("disk_spill_bytes") is not None
                    else (s.get("disk_spilled_bytes") or s.get("diskBytesSpilled"))
                ),
                jvm_gc_time_ms=s.get("jvm_gc_time_ms") or s.get("jvmGCTime"),
                executor_run_time_ms=s.get("executor_run_time_ms") or s.get("executorRunTime"),
                tasks=tasks,
                evidence_source=evidence_source,
            )
        )

    return RuntimeRun(
        run_id=raw.get("run_id", "run-001"),
        job_id=raw.get("job_id"),
        run_name=str(raw.get("run_name", raw.get("name", "pipeline_run"))),
        status=str(raw.get("status", raw.get("state", "SUCCESS"))),
        duration_seconds=duration_sec,
        stages=stages,
        executor_failures=list(raw.get("executor_failures", []) or []),
        cluster_utilization=raw.get("cluster_utilization"),
        evidence_source=evidence_source,
        collection_method=col_method,
    )
