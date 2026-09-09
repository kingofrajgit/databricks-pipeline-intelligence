"""Reformat long lines in src/dpif/runtime/analyzers.py."""
from pathlib import Path

target = Path("src/dpif/runtime/analyzers.py")
content = target.read_text(encoding="utf-8")

replacements = [
    (
        '"Shuffle occurred but input bytes metric was unavailable in the runtime evidence"',
        '"Shuffle occurred but input bytes metric was unavailable in the runtime evidence"',
    ),
    (
        '"Shuffle occurred but input bytes metric was unavailable in the runtime evidence"',
        '(\n'
        '                    "Shuffle occurred but input bytes metric was unavailable in "\n'
        '                    "the runtime evidence"\n'
        '                )',
    ),
    (
        '"Enable input stage metrics in Spark event log to evaluate shuffle-to-input ratio."',
        '(\n'
        '                    "Enable input stage metrics in Spark event log to evaluate "\n'
        '                    "shuffle-to-input ratio."\n'
        '                )',
    ),
    (
        '"Address task duration imbalance: check for partition size skew, enable Adaptive "\n'
        '                "Query Execution (AQE) skew join optimization, or adjust spark.sql.shuffle.partitions."',
        '"Address task duration imbalance: check for partition size skew, enable Adaptive "\n'
        '                "Query Execution (AQE) skew join optimization, or adjust "\n'
        '                "spark.sql.shuffle.partitions."',
    ),
    (
        'f"The supplied runtime evidence observed approximately {spill_gb:.2f} GB of memory spill"',
        'f"The supplied runtime evidence observed approximately {spill_gb:.2f} GB of "\n'
        '                f"memory spill"',
    ),
    (
        '"Increase worker executor memory, increase shuffle partitions to reduce partition size, "\n'
        '                "or optimize memory-heavy joins to prevent memory spill."',
        '"Increase worker executor memory, increase shuffle partitions to reduce partition "\n'
        '                "size, or optimize memory-heavy joins to prevent memory spill."',
    ),
    (
        'f"The supplied runtime evidence observed approximately {disk_spill_gb:.2f} GB of disk spill"',
        'f"The supplied runtime evidence observed approximately {disk_spill_gb:.2f} GB of "\n'
        '                f"disk spill"',
    ),
    (
        '"Disk spill causes severe disk I/O latency: increase executor memory, resize cluster "\n'
        '                "worker types with higher RAM-to-core ratios, or repartition workloads."',
        '"Disk spill causes severe disk I/O latency: increase executor memory, resize cluster "\n'
        '                "worker types with higher RAM-to-core ratios, or repartition workloads."',
    ),
    (
        'f"The run encountered {failed_tasks} failed task(s), {failed_stages} failed stage(s), "\n'
        '                f"run status: {run.status}"',
        'f"The run encountered {failed_tasks} failed task(s), "\n'
        '                f"{failed_stages} failed stage(s), run status: {run.status}"',
    ),
    (
        '"Investigate root cause of task failures: check executor logs for OOMs, node restarts, "\n'
        '                "transient network timeouts, or unhandled data parse exceptions."',
        '"Investigate root cause of task failures: check executor logs for OOMs, node restarts, "\n'
        '                "transient network timeouts, or unhandled data exceptions."',
    ),
    (
        '"""RUNTIME-PERF-010: Detect explicit executor failure events (lost, killed, heartbeat timeout)."""',
        '"""RUNTIME-PERF-010: Detect explicit executor failures (lost, killed, heartbeat timeout)."""',
    ),
    (
        'f"Executor failure event observed: {f.get(\'reason\', \'lost\')} on executor {f.get(\'executor_id\', \'unknown\')}"',
        'f"Executor failure event observed: {f.get(\'reason\', \'lost\')} "\n'
        '                f"on executor {f.get(\'executor_id\', \'unknown\')}"',
    ),
    (
        '"evidence": ["No cluster CPU/memory utilization metrics were present in the evidence"],',
        '"evidence": [\n'
        '                    "No cluster CPU/memory utilization metrics were present in the evidence"\n'
        '                ],',
    ),
    (
        '"recommendation": "Enable cluster gangia/metrics streaming to assess resource utilization.",',
        '"recommendation": (\n'
        '                    "Enable cluster metrics streaming to assess resource utilization."\n'
        '                ),',
    ),
    (
        '"""RUNTIME-PERF-012: Detect isolated long-tail tasks (P95/P99 stragglers) delaying stage completion."""',
        '"""RUNTIME-PERF-012: Detect isolated long-tail tasks delaying stage completion."""',
    ),
]

for old, new in replacements:
    assert old in content, f"Not found: {old[:40]}"
    content = content.replace(old, new, 1)

target.write_text(content, encoding="utf-8")
print("Done reformatting analyzers.py")
