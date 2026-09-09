"""Unit tests for Phase 7 Runtime Performance Models and Normalization."""

from __future__ import annotations

import pytest

from dpif.models import CollectionMethod
from dpif.runtime.models import (
    CorrelationResult,
    RuntimeRun,
    RuntimeStage,
    RuntimeTask,
    RuntimeTaskMetrics,
)
from dpif.runtime.normalization import (
    bytes_to_gb,
    calculate_distribution,
    ms_to_seconds,
    normalize_runtime_payload,
)


def test_runtime_task_metrics_defaults():
    m = RuntimeTaskMetrics()
    assert m.input_bytes == 0
    assert m.output_bytes == 0
    assert m.shuffle_read_bytes == 0
    assert m.shuffle_write_bytes == 0
    assert m.memory_spill_bytes == 0
    assert m.disk_spill_bytes == 0
    assert m.executor_run_time_ms == 0
    assert m.executor_cpu_time_ms == 0
    assert m.jvm_gc_time_ms == 0


def test_runtime_stage_properties():
    # Stage with direct properties
    s1 = RuntimeStage(
        stage_id=1,
        name="Stage 1",
        shuffle_read_bytes=1000,
        shuffle_write_bytes=2000,
        input_bytes=6000,
        jvm_gc_time_ms=500,
        executor_run_time_ms=10000,
    )
    assert s1.total_shuffle_bytes == 3000
    assert s1.shuffle_to_input_ratio == pytest.approx(0.5)
    assert s1.gc_ratio == pytest.approx(0.05)

    # Stage without input bytes -> shuffle_to_input_ratio is None
    s2 = RuntimeStage(stage_id=2, input_bytes=None)
    assert s2.shuffle_to_input_ratio is None

    # Stage with zero input bytes -> shuffle_to_input_ratio is None
    s3 = RuntimeStage(stage_id=3, input_bytes=0)
    assert s3.shuffle_to_input_ratio is None

    # Stage with tasks
    t1 = RuntimeTask(
        task_id=1,
        stage_id=4,
        duration_seconds=5.0,
        metrics=RuntimeTaskMetrics(
            shuffle_read_bytes=500,
            shuffle_write_bytes=500,
            jvm_gc_time_ms=100,
            executor_run_time_ms=2000,
        ),
    )
    t2 = RuntimeTask(
        task_id=2,
        stage_id=4,
        duration_seconds=7.0,
        metrics=RuntimeTaskMetrics(
            shuffle_read_bytes=300,
            shuffle_write_bytes=200,
            jvm_gc_time_ms=200,
            executor_run_time_ms=3000,
        ),
    )
    s4 = RuntimeStage(stage_id=4, tasks=[t1, t2])
    assert s4.total_shuffle_bytes == 1500
    assert s4.gc_ratio == pytest.approx(300 / 5000)
    assert s4.task_durations == [5.0, 7.0]


def test_runtime_run_properties():
    s1 = RuntimeStage(
        stage_id=1,
        task_count=10,
        failed_task_count=1,
        input_bytes=1000,
        output_bytes=500,
        shuffle_read_bytes=200,
        shuffle_write_bytes=300,
        memory_spill_bytes=400,
        disk_spill_bytes=100,
        jvm_gc_time_ms=1000,
        executor_run_time_ms=10000,
    )
    s2 = RuntimeStage(
        stage_id=2,
        task_count=20,
        failed_task_count=2,
        input_bytes=2000,
        output_bytes=800,
        shuffle_read_bytes=400,
        shuffle_write_bytes=600,
        memory_spill_bytes=200,
        disk_spill_bytes=50,
        jvm_gc_time_ms=2000,
        executor_run_time_ms=15000,
    )
    run = RuntimeRun(
        run_id="run-100",
        duration_seconds=42.5,
        stages=[s1, s2],
        cluster_utilization=0.75,
    )

    assert run.execution_duration_ms == 42500.0
    assert run.total_input_bytes == 3000
    assert run.total_output_bytes == 1300
    assert run.total_shuffle_bytes == 1500
    assert run.total_memory_spill_bytes == 600
    assert run.total_disk_spill_bytes == 150
    assert run.total_spill_bytes == 750
    assert run.total_failed_tasks == 3
    assert run.total_tasks == 30
    assert run.gc_ratio == pytest.approx(3000 / 25000)


def test_normalization_utils():
    assert bytes_to_gb(None) is None
    assert bytes_to_gb(1024 * 1024 * 1024) == 1.0
    assert ms_to_seconds(None) is None
    assert ms_to_seconds(5000) == 5.0

    # calculate_distribution
    empty_dist = calculate_distribution([])
    assert empty_dist["count"] == 0
    assert empty_dist["median"] == 0.0

    single_dist = calculate_distribution([10.0])
    assert single_dist["count"] == 1
    assert single_dist["median"] == 10.0
    assert single_dist["max"] == 10.0

    multi_dist = calculate_distribution([1.0, 2.0, 3.0, 4.0, 10.0])
    assert multi_dist["count"] == 5
    assert multi_dist["median"] == 3.0
    assert multi_dist["max"] == 10.0


def test_normalize_runtime_payload():
    payload = {
        "run_id": "test-run-1",
        "job_id": 42,
        "execution_duration_ms": 60000,
        "cluster_utilization": 0.85,
        "evidence_source": "FIXTURE",
        "stages": [
            {
                "stage_id": 1,
                "name": "Stage A",
                "duration_ms": 30000,
                "input_bytes": 5000,
                "task_durations": [2000, 3000, 4000],
                "task_input_bytes": [1000, 2000, 2000],
            }
        ],
    }
    run = normalize_runtime_payload(payload)
    assert run.run_id == "test-run-1"
    assert run.duration_seconds == 60.0
    assert run.collection_method == CollectionMethod.FIXTURE
    assert len(run.stages) == 1
    stage = run.stages[0]
    assert stage.duration_seconds == 30.0
    assert len(stage.tasks) == 3
    assert stage.task_durations == [2.0, 3.0, 4.0]


def test_correlation_result_model():
    cr = CorrelationResult(
        static_rule_id="CODE-PYSPARK-008",
        runtime_rule_ids=["RUNTIME-PERF-002", "RUNTIME-PERF-003"],
        status="RUNTIME_SUPPORTS_STATIC_RISK",
        description="Corroborated by high shuffle volume and ratio.",
        confidence=0.95,
        evidence=["Static: large dropDuplicates", "Runtime: High Shuffle Volume"],
    )
    assert cr.static_rule_id == "CODE-PYSPARK-008"
    assert len(cr.runtime_rule_ids) == 2
    assert cr.status == "RUNTIME_SUPPORTS_STATIC_RISK"
    assert cr.confidence == 0.95
