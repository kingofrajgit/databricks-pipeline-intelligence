"""Script to create all 14 runtime test fixtures."""
import json
from pathlib import Path

fixtures_dir = Path("tests/fixtures/runtime")
fixtures_dir.mkdir(parents=True, exist_ok=True)

# 1. healthy_run.json
# Normal shuffle (<20GB), normal ratio (<0.5), normal skew (<1.5), no spill, low GC (<5%), no failures, good utilization (0.85)
healthy_run = {
    "run_id": "run_healthy_001",
    "job_id": "job_101",
    "execution_duration_ms": 120000,
    "cluster_id": "cluster_abc",
    "spark_version": "14.3.x-scala2.12",
    "cluster_utilization": 0.85,
    "stages": [
        {
            "stage_id": 1,
            "name": "Scan and Filter",
            "num_tasks": 20,
            "executor_run_time_ms": 100000,
            "executor_cpu_time_ms": 80000,
            "input_bytes": 10 * 1024 * 1024 * 1024,  # 10 GB
            "output_bytes": 2 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 500 * 1024 * 1024,  # 500 MB
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 3000,  # 3% of 100s
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [5000] * 20,
            "task_input_bytes": [500 * 1024 * 1024] * 20,
        },
        {
            "stage_id": 2,
            "name": "Aggregate",
            "num_tasks": 20,
            "executor_run_time_ms": 60000,
            "executor_cpu_time_ms": 50000,
            "input_bytes": 500 * 1024 * 1024,
            "output_bytes": 50 * 1024 * 1024,
            "shuffle_read_bytes": 500 * 1024 * 1024,
            "shuffle_write_bytes": 0,
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 1500,  # 2.5%
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [3000] * 20,
            "task_input_bytes": [25 * 1024 * 1024] * 20,
        },
    ],
}

# 2. high_shuffle_run.json
# Shuffle write > 100 GB (e.g. 150 GB) -> FAIL PERF-001
high_shuffle_run = {
    "run_id": "run_high_shuffle_001",
    "job_id": "job_102",
    "execution_duration_ms": 450000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.80,
    "stages": [
        {
            "stage_id": 1,
            "name": "Heavy Shuffle Stage",
            "num_tasks": 100,
            "executor_run_time_ms": 400000,
            "input_bytes": 50 * 1024 * 1024 * 1024,
            "output_bytes": 120 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 150 * 1024 * 1024 * 1024,  # 150 GB > 100 GB
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 5000,
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [4000] * 100,
            "task_input_bytes": [500 * 1024 * 1024] * 100,
        }
    ],
}

# 3. high_shuffle_input_ratio_run.json
# Input: 10 GB, Shuffle: 40 GB -> Ratio = 4.0 (> 3.0 FAIL PERF-003)
high_shuffle_input_ratio_run = {
    "run_id": "run_high_ratio_001",
    "job_id": "job_103",
    "execution_duration_ms": 300000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.75,
    "stages": [
        {
            "stage_id": 1,
            "name": "Cartesian Product or Explosion",
            "num_tasks": 50,
            "executor_run_time_ms": 250000,
            "input_bytes": 10 * 1024 * 1024 * 1024,  # 10 GB
            "output_bytes": 35 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 40 * 1024 * 1024 * 1024,  # 40 GB -> ratio = 4.0
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 5000,
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [5000] * 50,
            "task_input_bytes": [200 * 1024 * 1024] * 50,
        }
    ],
}

# 4. task_imbalance_run.json
# Task duration max/median > 5.0 (e.g. median 1000ms, max 8000ms -> ratio 8.0 FAIL PERF-002)
task_imbalance_run = {
    "run_id": "run_imbalance_001",
    "job_id": "job_104",
    "execution_duration_ms": 180000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.70,
    "stages": [
        {
            "stage_id": 1,
            "name": "Imbalanced Stage",
            "num_tasks": 10,
            "executor_run_time_ms": 50000,
            "input_bytes": 5 * 1024 * 1024 * 1024,
            "output_bytes": 1 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 100 * 1024 * 1024,
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 1000,
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 8000],
            "task_input_bytes": [500 * 1024 * 1024] * 10,
        }
    ],
}

# 5. data_skew_run.json
# Task input bytes max/median > 5.0 (median 100MB, max 700MB -> ratio 7.0 FAIL PERF-004)
data_skew_run = {
    "run_id": "run_skew_001",
    "job_id": "job_105",
    "execution_duration_ms": 200000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.80,
    "stages": [
        {
            "stage_id": 1,
            "name": "Skewed Partition Stage",
            "num_tasks": 10,
            "executor_run_time_ms": 60000,
            "input_bytes": 1600 * 1024 * 1024,
            "output_bytes": 500 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 100 * 1024 * 1024,
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 1000,
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [2000] * 10,
            "task_input_bytes": [
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                100 * 1024 * 1024,
                700 * 1024 * 1024,
            ],
        }
    ],
}

# 6. memory_spill_run.json
# Spill memory > 20 GB (e.g. 35 GB -> FAIL PERF-005)
memory_spill_run = {
    "run_id": "run_mem_spill_001",
    "job_id": "job_106",
    "execution_duration_ms": 250000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.70,
    "stages": [
        {
            "stage_id": 1,
            "name": "High Memory Pressure Stage",
            "num_tasks": 20,
            "executor_run_time_ms": 200000,
            "input_bytes": 50 * 1024 * 1024 * 1024,
            "output_bytes": 10 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 5 * 1024 * 1024 * 1024,
            "memory_spilled_bytes": 35 * 1024 * 1024 * 1024,  # 35 GB
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 4000,
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [10000] * 20,
            "task_input_bytes": [2500 * 1024 * 1024] * 20,
        }
    ],
}

# 7. disk_spill_run.json
# Disk spill > 10 GB (e.g. 15 GB -> FAIL PERF-006)
disk_spill_run = {
    "run_id": "run_disk_spill_001",
    "job_id": "job_107",
    "execution_duration_ms": 320000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.70,
    "stages": [
        {
            "stage_id": 1,
            "name": "Disk Spill Stage",
            "num_tasks": 20,
            "executor_run_time_ms": 280000,
            "input_bytes": 50 * 1024 * 1024 * 1024,
            "output_bytes": 10 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 5 * 1024 * 1024 * 1024,
            "memory_spilled_bytes": 30 * 1024 * 1024 * 1024,
            "disk_spilled_bytes": 15 * 1024 * 1024 * 1024,  # 15 GB
            "jvm_gc_time_ms": 5000,
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [14000] * 20,
            "task_input_bytes": [2500 * 1024 * 1024] * 20,
        }
    ],
}

# 8. high_gc_run.json
# GC time 30,000ms out of 100,000ms executor time = 30% (> 20% FAIL PERF-008)
high_gc_run = {
    "run_id": "run_high_gc_001",
    "job_id": "job_108",
    "execution_duration_ms": 150000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.85,
    "stages": [
        {
            "stage_id": 1,
            "name": "GC Heavy Stage",
            "num_tasks": 20,
            "executor_run_time_ms": 100000,
            "input_bytes": 20 * 1024 * 1024 * 1024,
            "output_bytes": 5 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 1 * 1024 * 1024 * 1024,
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 30000,  # 30%
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [5000] * 20,
            "task_input_bytes": [1024 * 1024 * 1024] * 20,
        }
    ],
}

# 9. failed_tasks_retry_run.json
# Failed tasks > 10 (e.g. 15 -> FAIL PERF-009)
failed_tasks_retry_run = {
    "run_id": "run_task_retries_001",
    "job_id": "job_109",
    "execution_duration_ms": 200000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.75,
    "stages": [
        {
            "stage_id": 1,
            "name": "Retrying Stage",
            "num_tasks": 50,
            "executor_run_time_ms": 120000,
            "input_bytes": 10 * 1024 * 1024 * 1024,
            "output_bytes": 2 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 500 * 1024 * 1024,
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 2000,
            "failed_tasks": 15,  # 15 failed tasks
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [2400] * 50,
            "task_input_bytes": [200 * 1024 * 1024] * 50,
        }
    ],
}

# 10. executor_failure_run.json
# Executor failures > 0 (e.g. 2 -> FAIL PERF-010)
executor_failure_run = {
    "run_id": "run_executor_fail_001",
    "job_id": "job_110",
    "execution_duration_ms": 250000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.70,
    "stages": [
        {
            "stage_id": 1,
            "name": "Node Lost Stage",
            "num_tasks": 40,
            "executor_run_time_ms": 150000,
            "input_bytes": 15 * 1024 * 1024 * 1024,
            "output_bytes": 3 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 800 * 1024 * 1024,
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 3000,
            "failed_tasks": 5,
            "killed_tasks": 0,
            "executor_failures": 2,  # 2 executor loss events
            "task_durations": [3750] * 40,
            "task_input_bytes": [375 * 1024 * 1024] * 40,
        }
    ],
}

# 11. low_utilization_run.json
# Cluster utilization = 0.20 (< 0.30 -> FAIL PERF-011)
low_utilization_run = {
    "run_id": "run_low_util_001",
    "job_id": "job_111",
    "execution_duration_ms": 180000,
    "cluster_id": "cluster_overprovisioned",
    "cluster_utilization": 0.20,  # 20%
    "stages": [
        {
            "stage_id": 1,
            "name": "Underutilized Stage",
            "num_tasks": 8,
            "executor_run_time_ms": 36000,
            "input_bytes": 1 * 1024 * 1024 * 1024,
            "output_bytes": 200 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 50 * 1024 * 1024,
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 500,
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [4500] * 8,
            "task_input_bytes": [128 * 1024 * 1024] * 8,
        }
    ],
}

# 12. long_tail_run.json
# p95 / p50 task duration > 4.0 (e.g. 50 tasks: 45 tasks at 1000ms, 5 tasks at 6000ms -> p95=6000, p50=1000 -> ratio 6.0 FAIL PERF-012)
long_tail_run = {
    "run_id": "run_long_tail_001",
    "job_id": "job_112",
    "execution_duration_ms": 160000,
    "cluster_id": "cluster_abc",
    "cluster_utilization": 0.80,
    "stages": [
        {
            "stage_id": 1,
            "name": "Straggler Stage",
            "num_tasks": 50,
            "executor_run_time_ms": 75000,
            "input_bytes": 10 * 1024 * 1024 * 1024,
            "output_bytes": 2 * 1024 * 1024 * 1024,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 200 * 1024 * 1024,
            "memory_spilled_bytes": 0,
            "disk_spilled_bytes": 0,
            "jvm_gc_time_ms": 1500,
            "failed_tasks": 0,
            "killed_tasks": 0,
            "executor_failures": 0,
            "task_durations": [1000] * 45 + [6000] * 5,
            "task_input_bytes": [200 * 1024 * 1024] * 50,
        }
    ],
}

# 13. missing_metrics_run.json
# Missing input_bytes, executor_run_time, cluster_utilization -> UNKNOWN
missing_metrics_run = {
    "run_id": "run_missing_metrics_001",
    "job_id": "job_113",
    "stages": [
        {
            "stage_id": 1,
            "name": "Incomplete Stage Data",
            "num_tasks": 10,
            "task_durations": [],
            "task_input_bytes": [],
        }
    ],
}

# 14. malformed_run.json
# Missing stages, invalid data types
malformed_run = {
    "run_id": 999999,
    "unexpected_blob": "corrupted payload",
}

fixtures = {
    "healthy_run.json": healthy_run,
    "high_shuffle_run.json": high_shuffle_run,
    "high_shuffle_input_ratio_run.json": high_shuffle_input_ratio_run,
    "task_imbalance_run.json": task_imbalance_run,
    "data_skew_run.json": data_skew_run,
    "memory_spill_run.json": memory_spill_run,
    "disk_spill_run.json": disk_spill_run,
    "high_gc_run.json": high_gc_run,
    "failed_tasks_retry_run.json": failed_tasks_retry_run,
    "executor_failure_run.json": executor_failure_run,
    "low_utilization_run.json": low_utilization_run,
    "long_tail_run.json": long_tail_run,
    "missing_metrics_run.json": missing_metrics_run,
    "malformed_run.json": malformed_run,
}

for filename, content in fixtures.items():
    target = fixtures_dir / filename
    target.write_text(json.dumps(content, indent=2), encoding="utf-8")
    print(f"Wrote {target}")

print("All 14 fixtures generated successfully.")
