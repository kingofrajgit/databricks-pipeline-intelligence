"""Comprehensive unit tests for all 12 Phase 7 Runtime Performance Rules.

Covers:
- Positive (WARN and FAIL thresholds)
- Negative (healthy/nominal execution)
- Edge case (single task, zero volume, boundaries)
- Threshold override
- Missing metrics / UNKNOWN semantics
"""

from __future__ import annotations

import pytest

from dpif.models import CheckpointStatus, Rule
from dpif.rules.engine import load_rules
from dpif.runtime.models import (
    RuntimeRun,
    RuntimeStage,
    RuntimeTask,
    RuntimeTaskMetrics,
)


@pytest.fixture(scope="module")
def runtime_rules() -> dict[str, Rule]:
    all_rules = load_rules()
    return {r.rule_id: r for r in all_rules if r.rule_id.startswith("RUNTIME-PERF-")}


# ====================================================================
# RUNTIME-PERF-001: Excessive Stage Duration
# ====================================================================
def test_perf_001_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-001"]
    stage = RuntimeStage(stage_id=1, name="Long Stage", duration_seconds=800.0)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_001_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-001"]
    stage = RuntimeStage(stage_id=1, name="Severe Stage", duration_seconds=2000.0)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_001_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-001"]
    stage = RuntimeStage(stage_id=1, name="Fast Stage", duration_seconds=120.0)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_001_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-001"].model_copy(deep=True)
    rule.params["warn_seconds"] = 100.0
    stage = RuntimeStage(stage_id=1, name="Custom Stage", duration_seconds=150.0)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_001_missing_stages(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-001"]
    run = RuntimeRun(run_id="r1", stages=[])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


# ====================================================================
# RUNTIME-PERF-002: High Shuffle Volume
# ====================================================================
def test_perf_002_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-002"]
    stage = RuntimeStage(
        stage_id=1,
        shuffle_write_bytes=150 * 1024 * 1024 * 1024,  # 150 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_002_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-002"]
    stage = RuntimeStage(
        stage_id=1,
        shuffle_write_bytes=600 * 1024 * 1024 * 1024,  # 600 GB > 500 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_002_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-002"]
    stage = RuntimeStage(
        stage_id=1,
        shuffle_write_bytes=5 * 1024 * 1024 * 1024,  # 5 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_002_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-002"].model_copy(deep=True)
    rule.params["warn_gb"] = 2.0
    stage = RuntimeStage(
        stage_id=1,
        shuffle_write_bytes=3 * 1024 * 1024 * 1024,  # 3 GB > 2 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None


def test_perf_002_zero_shuffle(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-002"]
    stage = RuntimeStage(stage_id=1, shuffle_write_bytes=0, shuffle_read_bytes=0)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


# ====================================================================
# RUNTIME-PERF-003: Shuffle-to-Input Ratio Risk
# ====================================================================
def test_perf_003_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-003"]
    stage = RuntimeStage(
        stage_id=1,
        input_bytes=10 * 1024 * 1024 * 1024,  # 10 GB
        shuffle_write_bytes=25 * 1024 * 1024 * 1024,  # 25 GB -> ratio 2.5
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_003_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-003"]
    stage = RuntimeStage(
        stage_id=1,
        input_bytes=10 * 1024 * 1024 * 1024,  # 10 GB
        shuffle_write_bytes=60 * 1024 * 1024 * 1024,  # 60 GB -> ratio 6.0 > 5.0
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_003_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-003"]
    stage = RuntimeStage(
        stage_id=1,
        input_bytes=100 * 1024 * 1024 * 1024,
        shuffle_write_bytes=10 * 1024 * 1024 * 1024,  # ratio 0.1
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_003_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-003"].model_copy(deep=True)
    rule.params["warn_ratio"] = 1.2
    stage = RuntimeStage(
        stage_id=1,
        input_bytes=10 * 1024 * 1024 * 1024,
        shuffle_write_bytes=15 * 1024 * 1024 * 1024,  # ratio 1.5 > 1.2
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None


def test_perf_003_missing_input_unknown(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-003"]
    # When input is 0 or missing, it should emit UNKNOWN, not fake PASS or crash
    stage = RuntimeStage(
        stage_id=1,
        input_bytes=0,
        shuffle_write_bytes=10 * 1024 * 1024 * 1024,
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN
    assert finding.confidence == 0.0


# ====================================================================
# RUNTIME-PERF-004: Task Duration Imbalance
# ====================================================================
def test_perf_004_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-004"]
    tasks = [RuntimeTask(task_id=i, stage_id=1, duration_seconds=10.0) for i in range(9)] + [
        RuntimeTask(task_id=9, stage_id=1, duration_seconds=35.0)
    ]  # max/median = 3.5
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_004_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-004"]
    tasks = [RuntimeTask(task_id=i, stage_id=1, duration_seconds=10.0) for i in range(9)] + [
        RuntimeTask(task_id=9, stage_id=1, duration_seconds=60.0)
    ]  # max/median = 6.0 > 5.0
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_004_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-004"]
    tasks = [RuntimeTask(task_id=i, stage_id=1, duration_seconds=10.0) for i in range(10)]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_004_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-004"].model_copy(deep=True)
    rule.params["warn_ratio"] = 1.5
    tasks = [RuntimeTask(task_id=i, stage_id=1, duration_seconds=10.0) for i in range(9)] + [
        RuntimeTask(task_id=9, stage_id=1, duration_seconds=18.0)
    ]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None


def test_perf_004_insufficient_tasks(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-004"]
    # Single task cannot establish skew distribution
    stage = RuntimeStage(
        stage_id=1,
        tasks=[RuntimeTask(task_id=1, stage_id=1, duration_seconds=10.0)],
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


# ====================================================================
# RUNTIME-PERF-005: Data Skew Risk
# ====================================================================
def test_perf_005_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-005"]
    tasks = [
        RuntimeTask(
            task_id=i,
            stage_id=1,
            metrics=RuntimeTaskMetrics(input_bytes=100 * 1024 * 1024),
        )
        for i in range(9)
    ] + [
        RuntimeTask(
            task_id=9,
            stage_id=1,
            metrics=RuntimeTaskMetrics(input_bytes=350 * 1024 * 1024),  # ratio 3.5
        )
    ]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_005_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-005"]
    tasks = [
        RuntimeTask(
            task_id=i,
            stage_id=1,
            metrics=RuntimeTaskMetrics(input_bytes=100 * 1024 * 1024),
        )
        for i in range(9)
    ] + [
        RuntimeTask(
            task_id=9,
            stage_id=1,
            metrics=RuntimeTaskMetrics(input_bytes=700 * 1024 * 1024),  # ratio 7.0 > 5.0
        )
    ]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_005_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-005"]
    tasks = [
        RuntimeTask(
            task_id=i,
            stage_id=1,
            metrics=RuntimeTaskMetrics(input_bytes=100 * 1024 * 1024),
        )
        for i in range(10)
    ]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_005_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-005"].model_copy(deep=True)
    rule.params["warn_skew_ratio"] = 1.5
    tasks = [
        RuntimeTask(
            task_id=i,
            stage_id=1,
            metrics=RuntimeTaskMetrics(input_bytes=100 * 1024 * 1024),
        )
        for i in range(9)
    ] + [
        RuntimeTask(
            task_id=9,
            stage_id=1,
            metrics=RuntimeTaskMetrics(input_bytes=180 * 1024 * 1024),
        )
    ]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None


def test_perf_005_empty_inputs(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-005"]
    stage = RuntimeStage(stage_id=1, tasks=[])
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


# ====================================================================
# RUNTIME-PERF-006: Memory Spill Risk
# ====================================================================
def test_perf_006_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-006"]
    stage = RuntimeStage(
        stage_id=1,
        memory_spill_bytes=5 * 1024 * 1024 * 1024,  # 5 GB > 1 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_006_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-006"]
    stage = RuntimeStage(
        stage_id=1,
        memory_spill_bytes=25 * 1024 * 1024 * 1024,  # 25 GB > 20 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_006_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-006"]
    stage = RuntimeStage(
        stage_id=1,
        memory_spill_bytes=100 * 1024 * 1024,  # 100 MB < 1 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_006_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-006"].model_copy(deep=True)
    rule.params["warn_spill_gb"] = 0.05  # 50 MB
    stage = RuntimeStage(
        stage_id=1,
        memory_spill_bytes=80 * 1024 * 1024,  # 80 MB > 50 MB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None


def test_perf_006_zero_spill(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-006"]
    stage = RuntimeStage(stage_id=1, memory_spill_bytes=0)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


# ====================================================================
# RUNTIME-PERF-007: Disk Spill Risk
# ====================================================================
def test_perf_007_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-007"]
    stage = RuntimeStage(
        stage_id=1,
        disk_spill_bytes=2 * 1024 * 1024 * 1024,  # 2 GB > 0.5 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_007_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-007"]
    stage = RuntimeStage(
        stage_id=1,
        disk_spill_bytes=15 * 1024 * 1024 * 1024,  # 15 GB > 10 GB
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_007_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-007"]
    stage = RuntimeStage(
        stage_id=1,
        disk_spill_bytes=0,
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_007_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-007"].model_copy(deep=True)
    rule.params["warn_spill_gb"] = 0.01  # 10 MB
    stage = RuntimeStage(
        stage_id=1,
        disk_spill_bytes=20 * 1024 * 1024,
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None


def test_perf_007_zero_spill(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-007"]
    run = RuntimeRun(run_id="r1", stages=[])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


# ====================================================================
# RUNTIME-PERF-008: Excessive GC Time
# ====================================================================
def test_perf_008_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-008"]
    stage = RuntimeStage(
        stage_id=1,
        executor_run_time_ms=100000,
        jvm_gc_time_ms=12000,  # 12%
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_008_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-008"]
    stage = RuntimeStage(
        stage_id=1,
        executor_run_time_ms=100000,
        jvm_gc_time_ms=25000,  # 25% > 20%
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_008_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-008"]
    stage = RuntimeStage(
        stage_id=1,
        executor_run_time_ms=100000,
        jvm_gc_time_ms=3000,  # 3%
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_008_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-008"].model_copy(deep=True)
    rule.params["warn_gc_ratio"] = 0.05
    stage = RuntimeStage(
        stage_id=1,
        executor_run_time_ms=100000,
        jvm_gc_time_ms=6000,  # 6% > 5%
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None


def test_perf_008_missing_exec_time_unknown(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-008"]
    stage = RuntimeStage(
        stage_id=1,
        executor_run_time_ms=0,
        jvm_gc_time_ms=500,
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN
    assert finding.confidence == 0.0


# ====================================================================
# RUNTIME-PERF-009: Failed Task / Retry Risk
# ====================================================================
def test_perf_009_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-009"]
    stage = RuntimeStage(stage_id=1, failed_task_count=3)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_009_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-009"]
    stage = RuntimeStage(stage_id=1, failed_task_count=15, status="FAILED")
    run = RuntimeRun(run_id="r1", stages=[stage], status="FAILED")
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_009_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-009"]
    stage = RuntimeStage(stage_id=1, failed_task_count=0)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_009_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-009"].model_copy(deep=True)
    rule.params["max_failed_tasks"] = 5
    stage = RuntimeStage(stage_id=1, failed_task_count=3)  # < 5
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_009_zero_failures(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-009"]
    run = RuntimeRun(run_id="r1", stages=[])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


# ====================================================================
# RUNTIME-PERF-010: Executor Failure Risk
# ====================================================================
def test_perf_010_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-010"]
    run = RuntimeRun(
        run_id="r1",
        executor_failures=[{"executor_id": "1", "reason": "Heartbeat lost"}],
    )
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_010_fail(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-010"]
    run = RuntimeRun(
        run_id="r1",
        executor_failures=[
            {"executor_id": "1", "reason": "OOM"},
            {"executor_id": "2", "reason": "OOM"},
            {"executor_id": "3", "reason": "Node lost"},
        ],
    )
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_perf_010_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-010"]
    run = RuntimeRun(run_id="r1", executor_failures=[])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_010_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-010"]
    run = RuntimeRun(
        run_id="r1",
        executor_failures=[{"executor_id": "worker-2", "reason": "Spot termination"}],
    )
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert "Spot termination" in finding.evidence.evidence[0]


def test_perf_010_empty_failures(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-010"]
    run = RuntimeRun(run_id="r1")
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


# ====================================================================
# RUNTIME-PERF-011: Low Cluster Utilization
# ====================================================================
def test_perf_011_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-011"]
    run = RuntimeRun(run_id="r1", cluster_utilization=0.20)  # 20% < 30%
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_011_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-011"]
    run = RuntimeRun(run_id="r1", cluster_utilization=0.85)  # 85% > 30%
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_011_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-011"].model_copy(deep=True)
    rule.params["min_cpu_utilization"] = 0.15
    run = RuntimeRun(run_id="r1", cluster_utilization=0.20)  # 20% > 15%
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_011_missing_metrics_unknown(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-011"]
    run = RuntimeRun(run_id="r1", cluster_utilization=None)
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN
    assert finding.confidence == 0.0


def test_perf_011_dict_metrics(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-011"]
    run = RuntimeRun(
        run_id="r1",
        cluster_utilization={"avg_cpu_utilization": 0.18, "peak_cpu": 0.35},
    )
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


# ====================================================================
# RUNTIME-PERF-012: Long Tail Task Risk
# ====================================================================
def test_perf_012_warn(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-012"]
    # 20 tasks: 18 at 10.0s, 2 at 50.0s -> p95=50, p50=10 -> ratio 5.0 > 4.0
    tasks = [RuntimeTask(task_id=i, stage_id=1, duration_seconds=10.0) for i in range(18)] + [
        RuntimeTask(task_id=18, stage_id=1, duration_seconds=50.0),
        RuntimeTask(task_id=19, stage_id=1, duration_seconds=50.0),
    ]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_012_high_skew_triggers(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-012"]
    # 25 tasks: 20 at 5.0s, 5 at 45.0s -> p95=45, p50=5 -> ratio 9.0 > 4.0
    tasks = [RuntimeTask(task_id=i, stage_id=1, duration_seconds=5.0) for i in range(20)] + [
        RuntimeTask(task_id=20 + i, stage_id=1, duration_seconds=45.0) for i in range(5)
    ]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_perf_012_negative(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-012"]
    tasks = [RuntimeTask(task_id=i, stage_id=1, duration_seconds=10.0) for i in range(20)]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None


def test_perf_012_threshold_override(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-012"].model_copy(deep=True)
    rule.params["tail_ratio_threshold"] = 2.0
    tasks = [RuntimeTask(task_id=i, stage_id=1, duration_seconds=10.0) for i in range(18)] + [
        RuntimeTask(task_id=18, stage_id=1, duration_seconds=25.0),
        RuntimeTask(task_id=19, stage_id=1, duration_seconds=25.0),
    ]
    stage = RuntimeStage(stage_id=1, tasks=tasks)
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is not None


def test_perf_012_insufficient_tasks(runtime_rules):
    rule = runtime_rules["RUNTIME-PERF-012"]
    stage = RuntimeStage(
        stage_id=1,
        tasks=[RuntimeTask(task_id=1, stage_id=1, duration_seconds=10.0)],
    )
    run = RuntimeRun(run_id="r1", stages=[stage])
    finding = rule.evaluate({"runtime_run": run})
    assert finding is None
