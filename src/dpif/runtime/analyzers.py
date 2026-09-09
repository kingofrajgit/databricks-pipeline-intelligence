"""Runtime Performance Analyzers (Phase 7).

Pure, deterministic detector functions evaluating actual execution behavior
against configured performance thresholds and pipeline contracts.
Follows strict evidence-driven semantics: missing metrics yield UNKNOWN, never PASS.
"""

from __future__ import annotations

from typing import Any

from dpif.models import PipelineContract
from dpif.runtime.models import RuntimeRun
from dpif.runtime.normalization import (
    bytes_to_gb,
    calculate_distribution,
    normalize_runtime_payload,
)


def _to_run(data: Any) -> RuntimeRun | None:
    if isinstance(data, RuntimeRun):
        return data
    if isinstance(data, dict) and data:
        try:
            return normalize_runtime_payload(data)
        except Exception:
            return None
    return None


# ====================================================================
# RUNTIME-PERF-001: Excessive Stage Duration
# ====================================================================
def analyze_stage_duration(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    warn_seconds: float = 600.0,
    fail_seconds: float = 1800.0,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-001: Detect stages whose execution duration exceeds thresholds."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    excessive_stages = [s for s in run.stages if s.duration_seconds >= warn_seconds]
    if not excessive_stages:
        return []

    worst = max(excessive_stages, key=lambda s: s.duration_seconds)
    level = "HIGH" if worst.duration_seconds >= fail_seconds else "WARN"

    evidence_lines = [
        f"Stage '{s.name}' (id={s.stage_id}) ran for {s.duration_seconds:.1f}s "
        f"(warn={warn_seconds:.0f}s, fail={fail_seconds:.0f}s)"
        for s in excessive_stages
    ]

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "excessive_stage_count": len(excessive_stages),
                "max_stage_duration_seconds": worst.duration_seconds,
                "stages": [
                    {
                        "stage_id": s.stage_id,
                        "name": s.name,
                        "duration_seconds": s.duration_seconds,
                        "task_count": s.task_count,
                    }
                    for s in excessive_stages
                ],
            },
            "expected": {
                "max_stage_duration_warn_seconds": warn_seconds,
                "max_stage_duration_fail_seconds": fail_seconds,
            },
            "evidence": evidence_lines,
            "recommendation": (
                "Investigate stage execution bottlenecks: evaluate shuffle boundaries, "
                "partition sizing, and potential straggler tasks."
            ),
            "confidence": 0.9,
        }
    ]


# ====================================================================
# RUNTIME-PERF-002: High Shuffle Volume
# ====================================================================
def analyze_shuffle_volume(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    warn_gb: float = 100.0,
    fail_gb: float = 500.0,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-002: Detect substantial shuffle volume across stages."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    total_shuffle_bytes = run.total_shuffle_bytes
    total_shuffle_gb = bytes_to_gb(total_shuffle_bytes) or 0.0

    if total_shuffle_gb < warn_gb:
        return []

    level = "HIGH" if total_shuffle_gb >= fail_gb else "WARN"

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "total_shuffle_gb": round(total_shuffle_gb, 2),
                "total_shuffle_bytes": total_shuffle_bytes,
            },
            "expected": {
                "max_shuffle_warn_gb": warn_gb,
                "max_shuffle_fail_gb": fail_gb,
            },
            "evidence": [
                f"The supplied runtime evidence observed approximately {total_shuffle_gb:.1f} GB "
                f"of shuffle (read+write across {len(run.stages)} stages)"
            ],
            "recommendation": (
                "Optimize partitioning, pre-filter data before joins/aggregations, or evaluate "
                "broadcast joins for small lookup tables to reduce shuffle."
            ),
            "confidence": 0.95,
        }
    ]


# ====================================================================
# RUNTIME-PERF-003: Shuffle-to-Input Ratio Risk
# ====================================================================
def analyze_shuffle_input_ratio(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    warn_ratio: float = 2.0,
    fail_ratio: float = 5.0,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-003: Detect excessive shuffle volume relative to read input volume."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    total_input = run.total_input_bytes
    total_shuffle = run.total_shuffle_bytes

    if total_shuffle == 0:
        return []

    if total_input <= 0:
        # Input volume unavailable -> UNKNOWN (never assume 0 or fabricate ratio)
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {
                    "total_shuffle_bytes": total_shuffle,
                    "input_bytes": None,
                },
                "expected": {"input_bytes_available": True},
                "evidence": [
                    (
                        "Shuffle occurred but input bytes metric was unavailable in "
                        "the runtime evidence"
                    )
                ],
                "recommendation": (
                    "Enable input stage metrics in Spark event log to evaluate "
                    "shuffle-to-input ratio."
                ),
                "confidence": 0.0,
            }
        ]

    ratio = float(total_shuffle) / float(total_input)
    if ratio < warn_ratio:
        return []

    level = "HIGH" if ratio >= fail_ratio else "WARN"

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "shuffle_to_input_ratio": round(ratio, 2),
                "total_shuffle_gb": round(bytes_to_gb(total_shuffle) or 0.0, 2),
                "total_input_gb": round(bytes_to_gb(total_input) or 0.0, 2),
            },
            "expected": {
                "max_shuffle_input_ratio_warn": warn_ratio,
                "max_shuffle_input_ratio_fail": fail_ratio,
            },
            "evidence": [
                f"Shuffle volume ({bytes_to_gb(total_shuffle):.1f} GB) is {ratio:.1f}x "
                f"the input volume ({bytes_to_gb(total_input):.1f} GB)"
            ],
            "recommendation": (
                "Review multi-stage shuffles, broad joins, or Cartesian products amplifying "
                "data volume beyond the raw input size."
            ),
            "confidence": 0.9,
        }
    ]


# ====================================================================
# RUNTIME-PERF-004: Task Duration Imbalance
# ====================================================================
def analyze_task_duration_imbalance(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    warn_ratio: float = 3.0,
    fail_ratio: float = 6.0,
    min_tasks_to_evaluate: int = 4,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-004: Detect wide variance between median and max task duration in stages."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    imbalanced_stages: list[dict[str, Any]] = []

    for s in run.stages:
        durs = s.task_durations
        if len(durs) < min_tasks_to_evaluate:
            continue
        dist = calculate_distribution(durs)
        median = dist.get("median", 0.0)
        max_dur = dist.get("max", 0.0)
        if median > 0.0:
            ratio = max_dur / median
            if ratio >= warn_ratio:
                imbalanced_stages.append(
                    {
                        "stage_id": s.stage_id,
                        "name": s.name,
                        "ratio": ratio,
                        "median": median,
                        "max": max_dur,
                        "task_count": len(durs),
                    }
                )

    if not imbalanced_stages:
        return []

    worst = max(imbalanced_stages, key=lambda x: x["ratio"])
    level = "HIGH" if worst["ratio"] >= fail_ratio else "WARN"

    evidence_lines = [
        f"Stage {s['stage_id']} task duration ratio is {s['ratio']:.1f}x "
        f"(max={s['max']:.1f}s vs median={s['median']:.1f}s across {s['task_count']} tasks)"
        for s in imbalanced_stages
    ]

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "imbalanced_stage_count": len(imbalanced_stages),
                "worst_ratio": round(worst["ratio"], 2),
                "stages": imbalanced_stages,
            },
            "expected": {
                "max_task_duration_ratio_warn": warn_ratio,
                "max_task_duration_ratio_fail": fail_ratio,
            },
            "evidence": evidence_lines,
            "recommendation": (
                "Address task duration imbalance: check for partition size skew, enable Adaptive "
                "Query Execution (AQE) skew join optimization, or adjust "
                "spark.sql.shuffle.partitions."
            ),
            "confidence": 0.85,
        }
    ]


# ====================================================================
# RUNTIME-PERF-005: Data Skew Risk
# ====================================================================
def analyze_data_skew(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    warn_skew_ratio: float = 3.0,
    fail_skew_ratio: float = 6.0,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-005: Detect data skew via input or shuffle byte distribution across tasks."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    skewed_stages: list[dict[str, Any]] = []

    for s in run.stages:
        if len(s.tasks) < 4:
            continue
        task_bytes = [
            float(
                (t.metrics.input_bytes if t.metrics else 0)
                + (t.metrics.shuffle_read_bytes if t.metrics else 0)
            )
            for t in s.tasks
        ]
        if not any(b > 0 for b in task_bytes):
            continue
        dist = calculate_distribution(task_bytes)
        med = dist.get("median", 0.0)
        p99 = dist.get("p99", 0.0)
        max_b = dist.get("max", 0.0)
        if med > 0:
            ratio = max_b / med
            if ratio >= warn_skew_ratio:
                skewed_stages.append(
                    {
                        "stage_id": s.stage_id,
                        "name": s.name,
                        "skew_ratio": ratio,
                        "median_bytes": med,
                        "p99_bytes": p99,
                        "max_bytes": max_b,
                    }
                )

    if not skewed_stages:
        return []

    def _skew_key(x: dict[str, Any]) -> float:
        return float(x["skew_ratio"])

    worst = max(skewed_stages, key=_skew_key)
    worst_ratio: float = float(worst["skew_ratio"])
    worst_max: float = float(worst["max_bytes"])
    worst_med: float = float(worst["median_bytes"])
    level = "HIGH" if worst_ratio >= fail_skew_ratio else "WARN"

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "skewed_stage_count": len(skewed_stages),
                "worst_skew_ratio": round(worst_ratio, 2),
                "worst_max_mb": round(worst_max / (1024.0**2), 1),
                "worst_median_mb": round(worst_med / (1024.0**2), 1),
            },
            "expected": {
                "max_skew_ratio_warn": warn_skew_ratio,
                "max_skew_ratio_fail": fail_skew_ratio,
            },
            "evidence": [
                f"Observed data skew in Stage {s['stage_id']}: max task read "
                f"{float(s['max_bytes']) / (1024**2):.1f} MB vs median "
                f"{float(s['median_bytes']) / (1024**2):.1f} MB "
                f"({float(s['skew_ratio']):.1f}x ratio)"
                for s in skewed_stages
            ],
            "recommendation": (
                "Observed partition data skew: salt high-cardinality join keys, enable AQE "
                "skew join handling, or repartition on balanced columns."
            ),
            "confidence": 0.9,
        }
    ]


# ====================================================================
# RUNTIME-PERF-006: Memory Spill Risk
# ====================================================================
def analyze_memory_spill(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    warn_spill_gb: float = 1.0,
    fail_spill_gb: float = 10.0,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-006: Detect execution memory spill during joins or aggregations."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    spill_bytes = run.total_memory_spill_bytes
    spill_gb = bytes_to_gb(spill_bytes) or 0.0

    if spill_gb < warn_spill_gb:
        return []

    level = "HIGH" if spill_gb >= fail_spill_gb else "WARN"

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "memory_spill_gb": round(spill_gb, 2),
                "memory_spill_bytes": spill_bytes,
            },
            "expected": {
                "max_memory_spill_warn_gb": warn_spill_gb,
                "max_memory_spill_fail_gb": fail_spill_gb,
            },
            "evidence": [
                f"The supplied runtime evidence observed approximately {spill_gb:.2f} GB of "
                f"memory spill"
            ],
            "recommendation": (
                "Increase worker executor memory, increase shuffle partitions to reduce partition "
                "size, or optimize memory-heavy joins to prevent memory spill."
            ),
            "confidence": 0.9,
        }
    ]


# ====================================================================
# RUNTIME-PERF-007: Disk Spill Risk
# ====================================================================
def analyze_disk_spill(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    warn_spill_gb: float = 0.5,
    fail_spill_gb: float = 5.0,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-007: Detect disk spill when spilled memory exceeds local buffer capacity."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    disk_spill_bytes = run.total_disk_spill_bytes
    disk_spill_gb = bytes_to_gb(disk_spill_bytes) or 0.0

    if disk_spill_gb < warn_spill_gb:
        return []

    level = "HIGH" if disk_spill_gb >= fail_spill_gb else "WARN"

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "disk_spill_gb": round(disk_spill_gb, 2),
                "disk_spill_bytes": disk_spill_bytes,
            },
            "expected": {
                "max_disk_spill_warn_gb": warn_spill_gb,
                "max_disk_spill_fail_gb": fail_spill_gb,
            },
            "evidence": [
                f"The supplied runtime evidence observed approximately {disk_spill_gb:.2f} GB of "
                f"disk spill"
            ],
            "recommendation": (
                "Disk spill causes severe disk I/O latency: increase executor memory, "
                "resize cluster worker types with higher RAM-to-core ratios, or repartition "
                "workloads."
            ),
            "confidence": 0.95,
        }
    ]


# ====================================================================
# RUNTIME-PERF-008: Excessive GC Time
# ====================================================================
def analyze_gc_time(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    warn_gc_ratio: float = 0.10,
    fail_gc_ratio: float = 0.20,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-008: Detect high JVM Garbage Collection overhead during stage execution."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    total_gc_ms = 0
    total_exec_ms = 0

    for s in run.stages:
        gc = s.jvm_gc_time_ms
        exec_t = s.executor_run_time_ms
        if gc is None and s.tasks:
            gc = sum(t.metrics.jvm_gc_time_ms for t in s.tasks if t.metrics)
        if exec_t is None and s.tasks:
            exec_t = sum(t.metrics.executor_run_time_ms for t in s.tasks if t.metrics)
        if gc is not None:
            total_gc_ms += gc
        if exec_t is not None:
            total_exec_ms += exec_t

    if total_exec_ms <= 0:
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"total_gc_time_ms": total_gc_ms, "executor_run_time_ms": None},
                "expected": {"executor_time_available": True},
                "evidence": ["Executor runtime was missing or zero in execution evidence"],
                "recommendation": "Enable executor runtime metrics in Spark event log.",
                "confidence": 0.0,
            }
        ]

    gc_ratio = float(total_gc_ms) / float(total_exec_ms)
    if gc_ratio < warn_gc_ratio:
        return []

    level = "HIGH" if gc_ratio >= fail_gc_ratio else "WARN"

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "gc_time_ratio": round(gc_ratio, 3),
                "total_gc_time_ms": total_gc_ms,
                "executor_run_time_ms": total_exec_ms,
            },
            "expected": {
                "max_gc_ratio_warn": warn_gc_ratio,
                "max_gc_ratio_fail": fail_gc_ratio,
            },
            "evidence": [
                f"JVM GC consumed {gc_ratio * 100:.1f}% of total executor CPU/run time "
                f"({total_gc_ms / 1000.0:.1f}s GC vs {total_exec_ms / 1000.0:.1f}s executor time)"
            ],
            "recommendation": (
                "Tune JVM garbage collection: avoid large Python/JVM object conversions, "
                "increase off-heap memory, or switch to G1GC / Shenandoah garbage collectors."
            ),
            "confidence": 0.9,
        }
    ]


# ====================================================================
# RUNTIME-PERF-009: Failed Task / Retry Risk
# ====================================================================
def analyze_task_failures_and_retries(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    max_failed_tasks: int = 0,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-009: Detect failed task attempts and retry overhead during execution."""
    run = _to_run(runtime_data)
    if run is None:
        return []

    failed_tasks = run.total_failed_tasks
    failed_stages = sum(1 for s in run.stages if s.status.upper() in ("FAILED", "KILLED"))

    if failed_tasks <= max_failed_tasks and failed_stages == 0 and run.status.upper() == "SUCCESS":
        return []

    level = "CRITICAL" if failed_stages > 0 or run.status.upper() in ("FAILED", "ERROR") else "WARN"

    return [
        {
            "triggered": True,
            "level": level,
            "observed": {
                "failed_task_count": failed_tasks,
                "failed_stage_count": failed_stages,
                "run_status": run.status,
            },
            "expected": {"max_failed_tasks": max_failed_tasks, "run_status": "SUCCESS"},
            "evidence": [
                f"The run encountered {failed_tasks} failed task(s), "
                f"{failed_stages} failed stage(s), run status: {run.status}"
            ],
            "recommendation": (
                "Investigate root cause of task failures: check executor logs for OOMs, "
                "node restarts, transient network timeouts, or unhandled data exceptions."
            ),
            "confidence": 0.95,
        }
    ]


# ====================================================================
# RUNTIME-PERF-010: Executor Failure Risk
# ====================================================================
def analyze_executor_failures(
    runtime_data: Any,
    contract: PipelineContract | None = None,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-010: Detect explicit executor failures (lost, killed, heartbeat timeout)."""
    run = _to_run(runtime_data)
    if run is None:
        return []

    failures = run.executor_failures
    if not failures:
        return []

    return [
        {
            "triggered": True,
            "level": "HIGH",
            "observed": {"executor_failure_count": len(failures), "failures": failures},
            "expected": {"executor_failures": 0},
            "evidence": [
                f"Executor failure event observed: {f.get('reason', 'lost')} "
                f"on executor {f.get('executor_id', 'unknown')}"
                for f in failures
            ],
            "recommendation": (
                "Investigate infrastructure/spot node terminations, worker memory exhaustion, "
                "or network heartbeat timeouts causing executor loss."
            ),
            "confidence": 0.95,
        }
    ]


# ====================================================================
# RUNTIME-PERF-011: Low Cluster Utilization
# ====================================================================
def analyze_cluster_utilization(
    runtime_data: Any,
    contract: PipelineContract | None = None,
    min_cpu_utilization: float = 0.30,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-011: Detect underutilized compute resources when utilization evidence exists."""
    run = _to_run(runtime_data)
    if run is None:
        return []

    util = run.cluster_utilization
    if util is None:
        # No utilization evidence -> UNKNOWN (never fabricate CPU percentages)
        return [
            {
                "triggered": True,
                "level": "UNKNOWN",
                "observed": {"cluster_utilization": None},
                "expected": {"utilization_evidence_available": True},
                "evidence": [
                    "No cluster CPU/memory utilization metrics were present in the evidence"
                ],
                "recommendation": (
                    "Enable cluster metrics streaming to assess resource utilization."
                ),
                "confidence": 0.0,
            }
        ]

    if isinstance(util, (int, float)):
        avg_cpu = float(util)
    elif isinstance(util, dict):
        cpu_val = util.get("avg_cpu_utilization")
        if cpu_val is None:
            cpu_val = util.get("cpu_percent", 1.0)
        avg_cpu = float(cpu_val) if cpu_val is not None else 1.0
    else:
        avg_cpu = 1.0

    if avg_cpu >= min_cpu_utilization:
        return []

    return [
        {
            "triggered": True,
            "level": "WARN",
            "observed": {
                "cluster_cpu_utilization": round(avg_cpu, 2),
                "threshold": min_cpu_utilization,
            },
            "expected": {"min_cluster_utilization": min_cpu_utilization},
            "evidence": [
                f"Cluster CPU utilization is {avg_cpu * 100:.1f}%, below the recommended "
                f"minimum of {min_cpu_utilization * 100:.1f}%"
            ],
            "recommendation": (
                "Cluster appears over-provisioned. Downsize worker instance types, reduce "
                "worker count, or enable aggressive autoscaling to lower compute costs."
            ),
            "confidence": 0.85,
        }
    ]


# ====================================================================
# RUNTIME-PERF-012: Long Tail Task Risk
# ====================================================================
def analyze_long_tail_tasks(
    runtime_data: RuntimeRun | dict[str, Any] | None,
    contract: PipelineContract | None = None,
    tail_ratio_threshold: float = 3.0,
    min_tail_tasks: int = 5,
) -> list[dict[str, Any]]:
    """RUNTIME-PERF-012: Detect isolated long-tail tasks delaying stage completion."""
    run = _to_run(runtime_data)
    if run is None or not run.stages:
        return []

    tail_stages: list[dict[str, Any]] = []

    for s in run.stages:
        durs = s.task_durations
        if len(durs) < min_tail_tasks:
            continue
        dist = calculate_distribution(durs)
        med = dist.get("median", 0.0)
        p95 = dist.get("p95", 0.0)
        p99 = dist.get("p99", 0.0)
        max_t = dist.get("max", 0.0)
        if med > 0:
            tail_ratio = p95 / med
            if tail_ratio >= tail_ratio_threshold:
                tail_stages.append(
                    {
                        "stage_id": s.stage_id,
                        "name": s.name,
                        "tail_ratio": tail_ratio,
                        "median": med,
                        "p95": p95,
                        "p99": p99,
                        "max": max_t,
                        "task_count": len(durs),
                    }
                )

    if not tail_stages:
        return []

    def _tail_key(x: dict[str, Any]) -> float:
        return float(x["tail_ratio"])

    worst = max(tail_stages, key=_tail_key)
    worst_tail: float = float(worst["tail_ratio"])

    return [
        {
            "triggered": True,
            "level": "WARN",
            "observed": {
                "tail_stage_count": len(tail_stages),
                "worst_tail_ratio": round(worst_tail, 2),
                "stages": tail_stages,
            },
            "expected": {"max_tail_ratio": tail_ratio_threshold},
            "evidence": [
                f"Stage {s['stage_id']} exhibits long-tail task execution: P95 task duration "
                f"({s['p95']:.1f}s) is {s['tail_ratio']:.1f}x the median ({s['median']:.1f}s)"
                for s in tail_stages
            ],
            "recommendation": (
                "Enable Spark speculative execution (`spark.speculation=true`) to launch duplicate "
                "attempts for straggler tasks, and check for partition-level skew."
            ),
            "confidence": 0.85,
        }
    ]
