"""Databricks Environment Analyzers (Phase 6).

Pure detector functions comparing Expected vs Implemented vs Actual.
Each analyzer is deterministic, has no side-effects, and returns structured result dicts.
"""

from __future__ import annotations

import re
from typing import Any

from dpif.discovery.models import (
    DatabricksCluster,
    DatabricksJob,
    DatabricksTask,
)
from dpif.models import PipelineContract


def _to_job(data: Any) -> DatabricksJob | None:
    if isinstance(data, DatabricksJob):
        return data
    if isinstance(data, dict) and data:
        try:
            # Unwrap settings if wrapped in Databricks API response format
            job_id = data.get("job_id", 0)
            settings = data.get("settings", data)
            tasks_raw = settings.get("tasks", [])
            tasks: list[DatabricksTask] = []
            for t in tasks_raw:
                if isinstance(t, DatabricksTask):
                    tasks.append(t)
                elif isinstance(t, dict):
                    tasks.append(DatabricksTask.model_validate(t))
            sched = settings.get("schedule")
            return DatabricksJob(
                job_id=job_id,
                name=settings.get("name", data.get("name", "")),
                creator_user_name=data.get("creator_user_name", ""),
                tasks=tasks,
                schedule=sched,
                max_concurrent_runs=settings.get("max_concurrent_runs", 1),
                max_retries=int(settings.get("max_retries", data.get("max_retries", 0)) or 0),
                timeout_seconds=int(
                    settings.get("timeout_seconds", data.get("timeout_seconds", 0)) or 0
                ),
                job_clusters=settings.get("job_clusters", []),
                git_source=settings.get("git_source"),
                evidence_source=data.get("evidence_source", "DATABRICKS_API"),
            )
        except Exception:
            return None
    return None


def _to_cluster(data: Any) -> DatabricksCluster | None:
    if isinstance(data, DatabricksCluster):
        return data
    if isinstance(data, dict) and data:
        try:
            return DatabricksCluster(
                cluster_id=str(data.get("cluster_id", "cluster")),
                cluster_name=str(data.get("cluster_name", "")),
                spark_version=str(data.get("spark_version", "")),
                node_type_id=str(data.get("node_type_id", "")),
                driver_node_type_id=data.get("driver_node_type_id"),
                num_workers=data.get("num_workers"),
                autoscale=data.get("autoscale"),
                photon=bool(
                    data.get("photon") or str(data.get("runtime_engine", "")).upper() == "PHOTON"
                ),
                runtime_engine=str(data.get("runtime_engine", "STANDARD")),
                policy_id=data.get("policy_id"),
                autotermination_minutes=int(data.get("autotermination_minutes", 0) or 0),
                spark_conf=data.get("spark_conf", {}) or {},
                spark_env_vars=data.get("spark_env_vars", {}) or {},
                cluster_source=str(data.get("cluster_source", "JOB")),
                state=str(data.get("state", "RUNNING")),
                evidence_source=data.get("evidence_source", "DATABRICKS_API"),
            )
        except Exception:
            return None
    return None


# ====================================================================
# Job Analyzers
# ====================================================================


def analyze_job_retries(
    job_data: Any,
    contract: PipelineContract | None = None,
    min_retries: int = 1,
) -> list[dict[str, Any]]:
    """CONFIG-JOB-001: Detect production jobs/tasks lacking retry configuration."""
    job = _to_job(job_data)
    if job is None:
        return []

    # If contract specifies expected retries, use that
    expected_retries = min_retries
    if contract and contract.reliability:
        expected_retries = max(min_retries, contract.reliability.retry_count)

    if job.max_retries >= expected_retries:
        return []

    unretried_tasks = [
        t for t in job.tasks if max(t.max_retries, job.max_retries) < expected_retries
    ]
    if not unretried_tasks and job.tasks:
        return []

    task_details = [
        {
            "task_key": t.task_key,
            "actual_retries": t.max_retries,
            "expected_retries": expected_retries,
        }
        for t in unretried_tasks
    ] or [
        {"task_key": "job", "actual_retries": job.max_retries, "expected_retries": expected_retries}
    ]

    evidence_lines = [
        f"Task '{t.task_key}' has {t.max_retries} retries (expected >= {expected_retries})"
        for t in unretried_tasks
    ] or [f"Job has {job.max_retries} retries (expected >= {expected_retries})"]

    return [
        {
            "triggered": True,
            "level": "HIGH",
            "observed": {
                "tasks_without_retries": task_details,
                "total_tasks": len(job.tasks),
                "job_max_retries": job.max_retries,
            },
            "expected": {"min_retries": expected_retries},
            "evidence": evidence_lines,
            "recommendation": (
                f"Configure retry count (>= {expected_retries}) on production tasks to prevent "
                "transient failure pipeline interruptions."
            ),
            "confidence": 0.9,
        }
    ]


def analyze_task_timeouts(
    job_data: Any,
    contract: PipelineContract | None = None,
    min_timeout_seconds: int = 60,
) -> list[dict[str, Any]]:
    """CONFIG-JOB-002: Detect production tasks without an explicit timeout."""
    job = _to_job(job_data)
    if job is None:
        return []

    expected_timeout_sec = min_timeout_seconds
    if contract and contract.reliability and contract.reliability.timeout_minutes:
        expected_timeout_sec = max(
            min_timeout_seconds, int(contract.reliability.timeout_minutes * 60)
        )

    if job.timeout_seconds >= expected_timeout_sec:
        return []

    no_timeout_tasks = [
        t for t in job.tasks if max(t.timeout_seconds, job.timeout_seconds) < expected_timeout_sec
    ]
    if not no_timeout_tasks and job.tasks:
        return []

    task_details = [
        {"task_key": t.task_key, "actual_timeout_seconds": t.timeout_seconds}
        for t in no_timeout_tasks
    ] or [{"task_key": "job", "actual_timeout_seconds": job.timeout_seconds}]

    evidence_lines = [
        f"Task '{t.task_key}' has timeout {t.timeout_seconds}s "
        f"(expected >= {expected_timeout_sec}s)"
        for t in no_timeout_tasks
    ] or [f"Job has timeout {job.timeout_seconds}s (expected >= {expected_timeout_sec}s)"]

    return [
        {
            "triggered": True,
            "level": "WARN",
            "observed": {
                "tasks_without_timeout": task_details,
                "job_timeout_seconds": job.timeout_seconds,
            },
            "expected": {"timeout_configured": True, "min_timeout_seconds": expected_timeout_sec},
            "evidence": evidence_lines,
            "recommendation": (
                "Set task or job execution timeouts to prevent hanging jobs from consuming "
                "compute indefinitely."
            ),
            "confidence": 0.85,
        }
    ]


def analyze_excessive_retries(
    job_data: Any,
    contract: PipelineContract | None = None,
    max_allowed_retries: int = 5,
) -> list[dict[str, Any]]:
    """CONFIG-JOB-003: Detect unusually high retry counts causing resource drains."""
    job = _to_job(job_data)
    if job is None:
        return []

    excessive_tasks = [
        t for t in job.tasks if max(t.max_retries, job.max_retries) > max_allowed_retries
    ]
    is_excessive = bool(excessive_tasks) or job.max_retries > max_allowed_retries
    if not is_excessive:
        return []

    task_details = [
        {
            "task_key": t.task_key,
            "actual_retries": t.max_retries,
            "max_allowed": max_allowed_retries,
        }
        for t in excessive_tasks
    ] or [
        {"task_key": "job", "actual_retries": job.max_retries, "max_allowed": max_allowed_retries}
    ]

    evidence_lines = [
        f"Task '{t.task_key}' has {t.max_retries} retries, exceeding threshold "
        f"({max_allowed_retries})"
        for t in excessive_tasks
    ] or [f"Job max_retries ({job.max_retries}) exceeds safe threshold ({max_allowed_retries})"]

    return [
        {
            "triggered": True,
            "level": "MEDIUM",
            "observed": {"excessive_retry_tasks": task_details, "job_max_retries": job.max_retries},
            "expected": {
                "max_retries_le": max_allowed_retries,
                "max_allowed_retries": max_allowed_retries,
            },
            "evidence": evidence_lines,
            "recommendation": (
                f"Reduce retry count to <= {max_allowed_retries}. Excessive retries on persistent "
                "bugs cause prolonged resource waste and delayed SLA breach notifications."
            ),
            "confidence": 0.85,
        }
    ]


def analyze_concurrency(
    job_data: Any,
    contract: PipelineContract | None = None,
    max_allowed_concurrency: int = 1,
) -> list[dict[str, Any]]:
    """CONFIG-JOB-004: Detect unsafe/unbounded concurrency on scheduled pipelines."""
    job = _to_job(job_data)
    if job is None:
        return []

    # Streaming jobs may allow higher concurrency or continuous execution
    if contract:
        proc = getattr(contract, "processing", "")
        proc_str = proc.lower() if isinstance(proc, str) else getattr(proc, "type", "").lower()
        if proc_str == "streaming":
            return []

    if job.max_concurrent_runs > max_allowed_concurrency:
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "actual_max_concurrent_runs": job.max_concurrent_runs,
                    "max_concurrent_runs": job.max_concurrent_runs,
                    "schedule": job.schedule.model_dump() if job.schedule else None,
                },
                "expected": {"max_concurrent_runs": max_allowed_concurrency},
                "evidence": [
                    f"Job allows {job.max_concurrent_runs} concurrent runs "
                    f"(expected <= {max_allowed_concurrency})"
                ],
                "recommendation": (
                    "Limit max_concurrent_runs to 1 for scheduled ETL pipelines to prevent "
                    "race conditions, out-of-order writes, and target table corruption."
                ),
                "confidence": 0.8,
            }
        ]
    return []


def analyze_schedule_mismatch(
    job_data: Any,
    contract: PipelineContract | None = None,
) -> list[dict[str, Any]]:
    """CONFIG-JOB-005: Compare expected schedule from contract against Databricks schedule."""
    job = _to_job(job_data)
    if job is None or contract is None or contract.schedule is None:
        return []

    actual_sched = job.schedule
    if actual_sched is None:
        return []

    expected_sched = contract.schedule
    mismatches: list[str] = []

    # Check pause status according to environment
    env = getattr(contract, "environment", "production").lower()
    if env == "production" and actual_sched.pause_status.upper() == "PAUSED":
        mismatches.append("Production job schedule is PAUSED in Databricks (expected UNPAUSED)")
    elif (
        env in ("development", "dev", "staging") and actual_sched.pause_status.upper() == "UNPAUSED"
    ):
        mismatches.append(
            f"{env.capitalize()} job schedule is UNPAUSED in Databricks "
            "(expected PAUSED to prevent unintended runs)"
        )

    # Check quartz cron hour / frequency against contract
    cron = (actual_sched.quartz_cron_expression or "").strip()
    freq = (expected_sched.frequency or "").lower()
    if cron:
        parts = cron.split()
        if len(parts) >= 6:
            hour_part = parts[2]
            if expected_sched.time and ":" in expected_sched.time:
                expected_hour = str(int(expected_sched.time.split(":")[0]))
                if (
                    hour_part not in ("*", "?")
                    and hour_part != expected_hour
                    and not hour_part.startswith("*/")
                ):
                    mismatches.append(
                        f"Job quartz cron schedule hour '{hour_part}' does not match "
                        f"contract scheduled time '{expected_sched.time}'"
                    )
            if freq == "daily" and parts[1] == "*" and parts[2] == "*":
                mismatches.append(
                    f"Job quartz cron runs continuously every minute, but contract schedule "
                    f"is '{freq}'"
                )

    if mismatches:
        return [
            {
                "triggered": True,
                "level": "HIGH",
                "observed": {
                    "actual_schedule": actual_sched.model_dump() if actual_sched else None,
                    "is_paused": job.is_paused,
                },
                "expected": {
                    "frequency": expected_sched.frequency,
                    "time": expected_sched.time,
                    "timezone": expected_sched.timezone,
                    "pause_status": "PAUSED"
                    if env in ("development", "dev", "staging")
                    else "UNPAUSED",
                },
                "evidence": mismatches,
                "recommendation": (
                    "Unpause production job schedule or align quartz cron with contract schedule."
                ),
                "confidence": 0.9,
            }
        ]
    return []


def analyze_source_mismatch(
    job_data: Any,
    contract: PipelineContract | None = None,
) -> list[dict[str, Any]]:
    """CONFIG-JOB-006: Compare expected script/notebook path against actual task paths."""
    job = _to_job(job_data)
    if job is None or contract is None:
        return []

    expected_file = ""
    if contract.source and hasattr(contract.source, "config"):
        expected_file = contract.source.config.get("code_file", "")
    if not expected_file and hasattr(contract, "_job_raw"):
        expected_file = getattr(contract, "_job_raw", {}).get("code_file", "")

    if not expected_file:
        return []

    expected_basename = expected_file.split("/")[-1].split("\\")[-1]
    actual_paths = [t.source_path for t in job.tasks if t.source_path]

    # Check if expected code is in any actual task
    matches = any(
        expected_basename in (path or "") or (path or "").endswith(expected_basename)
        for path in actual_paths
    )
    if actual_paths and not matches:
        return [
            {
                "triggered": True,
                "level": "HIGH",
                "observed": {"actual_task_paths": actual_paths},
                "expected": {"expected_source_file": expected_file},
                "evidence": [
                    f"Job tasks execute {actual_paths}, but contract expects '{expected_file}'"
                ],
                "recommendation": (
                    "Ensure Databricks job task points to the intended production script."
                ),
                "confidence": 0.85,
            }
        ]
    return []


def analyze_task_dependencies(job_data: Any) -> list[dict[str, Any]]:
    """Analyze DAG for disconnected tasks or cycles."""
    job = _to_job(job_data)
    if job is None or len(job.tasks) <= 1:
        return []

    all_keys = {t.task_key for t in job.tasks}
    missing_deps = []
    for t in job.tasks:
        for dep in t.depends_on:
            if dep.task_key not in all_keys:
                missing_deps.append(
                    f"Task '{t.task_key}' depends on non-existent task '{dep.task_key}'"
                )

    if missing_deps:
        return [
            {
                "triggered": True,
                "level": "CRITICAL",
                "observed": {"missing_dependencies": missing_deps},
                "expected": {"all_dependencies_present": True},
                "evidence": missing_deps,
                "recommendation": "Fix job DAG: remove or correct invalid task dependencies.",
                "confidence": 1.0,
            }
        ]
    return []


# ====================================================================
# Cluster Analyzers
# ====================================================================


def analyze_cluster_runtime(
    cluster_data: Any,
    contract: PipelineContract | None = None,
    approved_versions: list[str] | None = None,
    min_version: str = "14.3",
) -> list[dict[str, Any]]:
    """CONFIG-CLUSTER-001: Compare configured DBR against allowed/supported versions."""
    cluster = _to_cluster(cluster_data)
    if cluster is None or not cluster.spark_version:
        return []

    spark_ver = cluster.spark_version
    # Extract version numbers, e.g. "15.4.x-scala2.12" -> 15.4
    m = re.match(r"^(\d+\.\d+)", spark_ver)
    ver_float = float(m.group(1)) if m else 0.0
    min_float = float(min_version)

    is_outdated = False
    evidence_msg = ""
    if approved_versions:
        if not any(spark_ver.startswith(v) for v in approved_versions):
            is_outdated = True
            evidence_msg = (
                f"Cluster uses DBR {spark_ver}, not in approved runtime list: {approved_versions}"
            )
    elif ver_float < min_float:
        is_outdated = True
        evidence_msg = (
            f"Cluster uses DBR {spark_ver}, below minimum supported baseline ({min_version})"
        )

    # Also check contract expected spark version
    expected_contract_ver = ""
    if contract:
        expected_contract_ver = getattr(contract, "cluster_spark_version", "") or getattr(
            contract, "_cluster_raw", {}
        ).get("spark_version", "")
    if expected_contract_ver and cluster.spark_version != expected_contract_ver:
        return [
            {
                "triggered": True,
                "level": "HIGH",
                "observed": {"spark_version": cluster.spark_version},
                "expected": {"spark_version": expected_contract_ver},
                "evidence": [
                    f"Cluster spark_version '{cluster.spark_version}' has mismatch with "
                    f"contract expected '{expected_contract_ver}'"
                ],
                "recommendation": (
                    f"Update cluster spark_version to match contract ({expected_contract_ver})."
                ),
                "confidence": 0.95,
            }
        ]

    if is_outdated:
        level = "CRITICAL" if ver_float < 14.0 else "HIGH"
        return [
            {
                "triggered": True,
                "level": level,
                "observed": {"spark_version": spark_ver, "version_number": ver_float},
                "expected": {
                    "min_version": min_version,
                    "approved_versions": approved_versions or [f">={min_version}"],
                },
                "evidence": [evidence_msg],
                "recommendation": (
                    f"Upgrade Databricks Runtime to supported LTS version (>= {min_version}) "
                    "to ensure security patches and Spark performance fixes."
                ),
                "confidence": 0.95,
            }
        ]
    return []


def analyze_autoscaling(
    cluster_data: Any,
    contract: PipelineContract | None = None,
    require_autoscaling: bool = False,
) -> list[dict[str, Any]]:
    """CONFIG-CLUSTER-002: Detect missing autoscaling on variable workloads."""
    cluster = _to_cluster(cluster_data)
    if cluster is None:
        return []

    # Only flag if contract or parameter explicitly requires autoscaling
    autoscale_needed = require_autoscaling
    if contract:
        raw_cluster = getattr(contract, "_cluster_raw", {}) or {}
        if raw_cluster.get("require_autoscaling") or raw_cluster.get("autoscale_required"):
            autoscale_needed = True
        if contract.source and getattr(contract.source, "growth_rate_percent", 0) > 20:
            autoscale_needed = True

    if not cluster.is_autoscaling and autoscale_needed:
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {"num_workers": cluster.num_workers, "is_autoscaling": False},
                "expected": {"autoscaling_enabled": True},
                "evidence": [
                    f"Cluster uses fixed worker allocation ({cluster.num_workers or 0} workers) "
                    "without autoscaling"
                ],
                "recommendation": (
                    "Enable cluster autoscaling to dynamically handle varying data volumes "
                    "and optimize compute costs."
                ),
                "confidence": 0.8,
            }
        ]
    return []


def analyze_autoscaling_range(
    cluster_data: Any,
    contract: PipelineContract | None = None,
    max_ratio: float = 10.0,
    max_workers_limit: int = 128,
) -> list[dict[str, Any]]:
    """CONFIG-CLUSTER-003: Detect invalid or excessively large autoscaling range."""
    cluster = _to_cluster(cluster_data)
    if cluster is None or not cluster.is_autoscaling or not cluster.autoscale:
        return []

    min_w = cluster.autoscale.min_workers
    max_w = cluster.autoscale.max_workers

    issues = []
    if min_w > max_w:
        issues.append(f"Invalid range: min_workers ({min_w}) > max_workers ({max_w})")
    elif min_w < 0 or max_w <= 0:
        issues.append(f"Invalid non-positive worker count: min={min_w}, max={max_w}")
    elif max_w > max_workers_limit:
        issues.append(f"max_workers ({max_w}) exceeds limit ({max_workers_limit})")
    elif min_w > 0 and (max_w / min_w) > max_ratio:
        issues.append(
            f"Autoscaling range ratio ({max_w / min_w:.1f}x) exceeds safe limit ({max_ratio:.1f}x)"
        )

    if issues:
        level = "CRITICAL" if min_w > max_w or max_w <= 0 else "WARN"
        return [
            {
                "triggered": True,
                "level": level,
                "observed": {"min_workers": min_w, "max_workers": max_w},
                "expected": {
                    "min_le_max": True,
                    "max_ratio": max_ratio,
                    "max_workers_le": max_workers_limit,
                },
                "evidence": issues,
                "recommendation": (
                    "Configure realistic min and max worker boundaries to avoid launch failures."
                ),
                "confidence": 0.9,
            }
        ]
    return []


def analyze_photon(
    cluster_data: Any,
    contract: PipelineContract | None = None,
) -> list[dict[str, Any]]:
    """CONFIG-CLUSTER-004: Missing Photon where required by contract or workload policy."""
    cluster = _to_cluster(cluster_data)
    if cluster is None:
        return []

    contract_requires_photon = False
    if contract:
        raw_cluster = getattr(contract, "_cluster_raw", {}) or {}
        target_cfg = getattr(contract.target, "config", {}) or {}
        contract_requires_photon = bool(
            raw_cluster.get("photon")
            or str(raw_cluster.get("runtime_engine", "")).upper() == "PHOTON"
            or target_cfg.get("photon")
        )

    if contract_requires_photon and not cluster.is_photon:
        return [
            {
                "triggered": True,
                "level": "HIGH",
                "observed": {
                    "photon_enabled": cluster.photon,
                    "runtime_engine": cluster.runtime_engine,
                },
                "expected": {"photon_enabled": True},
                "evidence": [
                    "Pipeline contract requires Photon, but actual cluster has Photon disabled"
                ],
                "recommendation": (
                    "Enable Photon runtime engine on the cluster to meet contract expectations."
                ),
                "confidence": 0.95,
            }
        ]
    return []


def analyze_cluster_policy(
    cluster_data: Any,
    policy_data: Any,
) -> list[dict[str, Any]]:
    """CONFIG-CLUSTER-005: Compare actual cluster configuration against cluster policy."""
    cluster = _to_cluster(cluster_data)
    if cluster is None or not policy_data:
        return []

    policy_id = (
        policy_data.get("policy_id", "")
        if isinstance(policy_data, dict)
        else getattr(policy_data, "policy_id", "")
    )
    if policy_id and (not cluster.policy_id or cluster.policy_id != policy_id):
        return [
            {
                "triggered": True,
                "level": "HIGH",
                "observed": {"cluster_policy_id": cluster.policy_id},
                "expected": {"policy_id": policy_id},
                "evidence": [
                    f"Cluster does not use required cluster policy '{policy_id}' "
                    f"(actual: '{cluster.policy_id or 'none'}')"
                ],
                "recommendation": (
                    f"Assign cluster policy '{policy_id}' to enforce governance boundaries."
                ),
                "confidence": 0.95,
            }
        ]

    definition = (
        policy_data.get("definition", {})
        if isinstance(policy_data, dict)
        else getattr(policy_data, "definition", {})
    )
    violations: list[str] = []

    for key, rule in definition.items():
        rule_type = rule.get("type")
        actual_val = getattr(cluster, key, None)
        if actual_val is None:
            actual_val = cluster.spark_conf.get(key)

        if rule_type == "fixed":
            expected_val = rule.get("value")
            if actual_val is not None and str(actual_val).upper() != str(expected_val).upper():
                violations.append(
                    f"Property '{key}': actual '{actual_val}' != policy value '{expected_val}'"
                )
        elif rule_type == "range":
            max_val = rule.get("maxValue")
            min_val = rule.get("minValue")
            if actual_val is not None and isinstance(actual_val, (int, float)):
                if max_val is not None and actual_val > max_val:
                    violations.append(
                        f"Property '{key}': actual '{actual_val}' exceeds max '{max_val}'"
                    )
                if min_val is not None and actual_val < min_val:
                    violations.append(
                        f"Property '{key}': actual '{actual_val}' below policy minimum '{min_val}'"
                    )
        elif rule_type == "regex":
            pattern = rule.get("pattern", "")
            if actual_val and pattern and not re.search(pattern, str(actual_val)):
                violations.append(
                    f"Property '{key}': actual '{actual_val}' does not match pattern '{pattern}'"
                )

    if violations:
        return [
            {
                "triggered": True,
                "level": "HIGH",
                "observed": {"policy_violations": violations},
                "expected": {"complies_with_policy": True},
                "evidence": [f"Cluster violates policy: {v}" for v in violations],
                "recommendation": "Align cluster settings with workspace cluster policy rules.",
                "confidence": 0.9,
            }
        ]
    return []


def analyze_autotermination(
    cluster_data: Any,
    contract: PipelineContract | None = None,
    max_autotermination_minutes: int = 60,
) -> list[dict[str, Any]]:
    """CONFIG-CLUSTER-006: Missing auto-termination on non-job/all-purpose clusters."""
    cluster = _to_cluster(cluster_data)
    if cluster is None:
        return []

    # Job clusters automatically terminate when job ends; rule applies to all-purpose / UI clusters
    if cluster.cluster_source.upper() == "JOB":
        return []

    if (
        cluster.autotermination_minutes <= 0
        or cluster.autotermination_minutes > max_autotermination_minutes
    ):
        return [
            {
                "triggered": True,
                "level": "WARN",
                "observed": {
                    "autotermination_minutes": cluster.autotermination_minutes,
                    "cluster_source": cluster.cluster_source,
                },
                "expected": {
                    "autotermination_enabled": True,
                    "max_autotermination_minutes": max_autotermination_minutes,
                },
                "evidence": [
                    f"Cluster '{cluster.cluster_name or cluster.cluster_id}' auto-termination "
                    f"is {cluster.autotermination_minutes}m "
                    f"(expected <= {max_autotermination_minutes}m)"
                ],
                "recommendation": (
                    f"Enable auto-termination (<= {max_autotermination_minutes}m) on all-purpose "
                    "clusters to avoid runaway idle infrastructure costs."
                ),
                "confidence": 0.85,
            }
        ]
    return []
