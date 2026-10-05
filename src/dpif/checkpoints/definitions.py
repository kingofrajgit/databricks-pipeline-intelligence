"""Concrete offline checkpoint builders (CP-001 ... CP-024).

Every builder follows the strict UNKNOWN rule: when required information
is unavailable the checkpoint returns UNKNOWN — never PASS.
"""

from __future__ import annotations

import ast
import re
from typing import Any

from dpif.models import (
    AnalysisMethod,
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    EvidenceRecord,
    PipelineContract,
    Severity,
)


def _unknown(checkpoint_id: str, name: str, category: str, reason: str) -> Checkpoint:
    return Checkpoint(
        checkpoint_id=checkpoint_id,
        name=name,
        category=category,
        status=CheckpointStatus.UNKNOWN,
        severity=Severity.INFO,
        score=0.0,
        evidence=EvidenceRecord(
            rule_id=checkpoint_id,
            status=CheckpointStatus.UNKNOWN,
            severity=Severity.INFO,
            observed={},
            expected={},
            evidence=[],
            recommendation=reason,
            confidence=0.0,
            method=AnalysisMethod.UNAVAILABLE,
        ),
        assumptions={"unknown-reason": reason},
    )


def cp001_source(contract: PipelineContract | None) -> Checkpoint:
    """Skeleton: SOURCE-* evaluator rules produce findings; engine aggregates.

    Missing source information stays UNKNOWN (never PASS).
    """
    if contract is None or contract.source is None:
        return _unknown("CP-001", "Source Validation", "source", "No source information available.")
    src = contract.source
    try:
        source_type = src.type.value  # validates enum
    except Exception:
        return _unknown(
            "CP-001", "Source Validation", "source", f"Unknown source type: {src.type!r}."
        )
    if not src.path and not src.config:
        return _unknown("CP-001", "Source Validation", "source", "Source path/config missing.")
    fmt = src.format.value if hasattr(src.format, "value") else str(src.format)
    return Checkpoint(
        checkpoint_id="CP-001",
        name="Source Validation",
        category="source",
        status=CheckpointStatus.PASS,
        severity=Severity.INFO,
        score=1.0,
        evidence=EvidenceRecord(
            rule_id="CP-001",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            observed={"source_type": source_type, "path": src.path, "format": fmt},
            expected={"supported_source": True},
            evidence=["pipeline contract source section"],
            recommendation="",
            confidence=0.9,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp003_schema_drift(
    contract: PipelineContract | None,
    profile: DataProfile | None,
) -> Checkpoint:
    """Skeleton: DATA-* evaluator rules produce findings; engine aggregates.

    Missing profile metadata or contract schema stays UNKNOWN (never PASS).
    """
    if contract is None or contract.source is None or contract.source.schema_definition is None:
        return _unknown(
            "CP-003",
            "Schema Drift Validation",
            "data",
            "No contract source or schema definition available.",
        )
    if profile is None or not profile.has_sufficient_metadata:
        unknown = _unknown(
            "CP-003",
            "Schema Drift Validation",
            "data",
            "No data profile metadata available for drift detection.",
        )
        unknown.depends_on = ["CP-001", "CP-007"]
        return unknown

    expected = contract.source.schema_definition.to_dict()
    observed = profile.to_dict()

    # Delegate to the schema drift analyzer (DATA-003 rule)
    from dpif.sql.analyzers import analyze_schema_drift

    result = analyze_schema_drift(
        expected,
        {"columns": observed.get("schema_columns", [])},
    )

    if result["status"] == "UNKNOWN":
        unknown = _unknown(
            "CP-003",
            "Schema Drift Validation",
            "data",
            "Insufficient schema information for drift detection.",
        )
        unknown.depends_on = ["CP-001", "CP-007"]
        return unknown

    status_map = {
        "PASS": CheckpointStatus.PASS,
        "WARN": CheckpointStatus.WARN,
        "FAIL": CheckpointStatus.FAIL,
        "UNKNOWN": CheckpointStatus.UNKNOWN,
    }
    severity_map = {
        "PASS": Severity.INFO,
        "WARN": Severity.MEDIUM,
        "FAIL": Severity.HIGH,
        "UNKNOWN": Severity.INFO,
    }
    status = status_map.get(result["status"], CheckpointStatus.UNKNOWN)
    severity = severity_map.get(result["status"], Severity.INFO)

    cp = Checkpoint(
        checkpoint_id="CP-003",
        name="Schema Drift Validation",
        category="data",
        status=status,
        severity=severity,
        score=1.0 if result["status"] == "PASS" else (0.5 if result["status"] == "WARN" else 0.2),
        depends_on=["CP-001", "CP-007"],
        evidence=EvidenceRecord(
            rule_id="CP-003",
            status=status,
            severity=severity,
            observed={
                "missing_columns": result.get("missing", []),
                "unexpected_columns": result.get("unexpected", []),
                "type_changes": result.get("type_changes", []),
                "nullable_changes": result.get("nullable_changes", []),
            },
            expected={"schema_match": True},
            evidence=[
                f"Missing columns: {result.get('missing', [])}" if result.get("missing") else "",
                f"Unexpected columns: {result.get('unexpected', [])}"
                if result.get("unexpected")
                else "",
                f"Type changes: {result.get('type_changes', [])}"
                if result.get("type_changes")
                else "",
                f"Nullable changes: {result.get('nullable_changes', [])}"
                if result.get("nullable_changes")
                else "",
            ],
            recommendation=(
                (
                    "Align the source schema with the contract schema. "
                    "Missing columns may indicate data loss; unexpected columns "
                    "may indicate schema evolution; type changes may break "
                    "downstream consumers."
                )
                if result["status"] != "PASS"
                else ""
            ),
            confidence=0.85,
            method=AnalysisMethod.METADATA,
        ),
        assumptions=(
            {"unknown-reason": "Insufficient schema information"}
            if result["status"] == "UNKNOWN"
            else {}
        ),
    )
    return cp


def cp007_data(
    profile: DataProfile | None,
    small_file_threshold_kb: float = 1000.0,
) -> Checkpoint:
    """Skeleton: DATA-* evaluator rules produce findings; engine aggregates.

    Missing profile metadata stays UNKNOWN (never PASS).
    """
    if profile is None or not profile.has_sufficient_metadata:
        unknown = _unknown(
            "CP-007", "Data Volume Validation", "data", "No data profile metadata available."
        )
        unknown.depends_on = ["CP-001"]
        return unknown
    return Checkpoint(
        checkpoint_id="CP-007",
        name="Data Volume Validation",
        category="data",
        status=CheckpointStatus.PASS,
        severity=Severity.INFO,
        score=1.0,
        depends_on=["CP-001"],
        evidence=EvidenceRecord(
            rule_id="CP-007",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            observed={
                "total_gb": profile.total_gb,
                "file_count": profile.file_count,
                "average_file_size_kb": profile.average_file_size_kb,
                "median_file_size_kb": profile.median_file_size_kb,
                "p95_file_size_kb": profile.p95_file_size_kb,
                "collection_method": profile.collection_method.value,
                "evidence_source": profile.evidence_source or "profile metadata",
            },
            expected={"sufficient_metadata": True},
            evidence=["data profile metadata"],
            recommendation="",
            confidence=0.85,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp004_code(code_text: str = "") -> Checkpoint:
    """Skeleton — the engine executes code rules against ``code_snippet``."""
    if not code_text.strip():
        return _unknown(
            "CP-004", "Code Validation", "code", "No code available for static analysis."
        )
    return Checkpoint(checkpoint_id="CP-004", name="Code Validation", category="code")


def cp009_cluster(cluster_config: dict[str, Any] | None) -> Checkpoint:
    if not cluster_config:
        return _unknown(
            "CP-009", "Cluster Validation", "cluster", "No cluster configuration available."
        )
    # Phase 6 (B1): an arbitrary non-empty mapping is not cluster evidence.
    # PASS requires at least one meaningful cluster field (worker sizing,
    # node type, runtime version, policy/pool identity, or spark config).
    _meaningful_cluster_keys = (
        "num_workers",
        "worker_count",
        "autoscale",
        "node_type",
        "node_type_id",
        "driver_node_type_id",
        "spark_version",
        "policy_id",
        "instance_pool_id",
        "cluster_id",
        "spark_conf",
    )
    if not any(cluster_config.get(k) for k in _meaningful_cluster_keys):
        return _unknown(
            "CP-009",
            "Cluster Validation",
            "cluster",
            "Cluster configuration present but no meaningful cluster evidence "
            "(node type, spark version, worker count, policy/pool, or spark config).",
        )
    workers = cluster_config.get("num_workers", cluster_config.get("worker_count"))
    if workers is None and cluster_config.get("autoscale"):
        workers = cluster_config["autoscale"].get("max_workers")
    findings_note = []
    status, severity, score, conf = CheckpointStatus.PASS, Severity.INFO, 1.0, 0.7
    if isinstance(workers, (int, float)) and workers <= 0:
        status, severity, score, conf = CheckpointStatus.FAIL, Severity.HIGH, 0.2, 0.8
        findings_note.append("worker count must be positive")
    return Checkpoint(
        checkpoint_id="CP-009",
        name="Cluster Validation",
        category="cluster",
        status=status,
        severity=severity,
        score=score,
        evidence=EvidenceRecord(
            rule_id="CP-009",
            status=status,
            severity=severity,
            observed=dict(cluster_config),
            expected={"num_workers": "> 0"},
            evidence=["cluster configuration"] if cluster_config else [],
            recommendation="; ".join(findings_note),
            confidence=conf,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp010_scalability(
    contract: PipelineContract | None = None,
    data_profile: DataProfile | None = None,
    cluster_config: dict[str, Any] | None = None,
    runtime_data: Any | None = None,
    code_text: str = "",
    historical_runs: list[Any] | None = None,
) -> Checkpoint:
    """CP-010: Scalability Validation.

    Evaluates whether pipeline design and observed evidence support expected, peak,
    and long-term growth workloads without capacity or SLA bottlenecks.
    Strict UNKNOWN: Returns UNKNOWN if no contract or volume evidence is available.
    """
    if contract is None and data_profile is None and runtime_data is None:
        return _unknown(
            "CP-010",
            "Scalability Validation",
            "scalability",
            "No contract, data profile, or runtime execution telemetry "
            "available for scalability analysis.",
        )

    # Phase 6 (B1): object presence alone is not volume evidence. At least
    # one positive volume signal (contract expected/peak, profile total, or
    # measured runtime bytes) is required; all-None/all-zero stays UNKNOWN.
    def _positive_volume(value: Any) -> bool:
        return isinstance(value, (int, float)) and value > 0

    _expected_gb = contract.expected_daily_volume_gb if contract else None
    _peak_gb = contract.peak_daily_volume_gb if contract else None
    _profile_gb = data_profile.total_gb if data_profile else None
    _has_volume = (
        _positive_volume(_expected_gb) or _positive_volume(_peak_gb) or _positive_volume(_profile_gb)
    )
    if not _has_volume:
        try:
            _rt_in = getattr(runtime_data, "total_input_bytes", None)
            _rt_out = getattr(runtime_data, "total_output_bytes", None)
            if isinstance(runtime_data, dict):
                _stages = runtime_data.get("stages") or []
                _rt_in = sum(int(s.get("input_bytes") or 0) for s in _stages if isinstance(s, dict))
                _rt_out = sum(int(s.get("output_bytes") or 0) for s in _stages if isinstance(s, dict))
            _has_volume = _positive_volume(_rt_in) or _positive_volume(_rt_out)
        except (TypeError, ValueError, AttributeError):
            _has_volume = False
    if not _has_volume:
        return _unknown(
            "CP-010",
            "Scalability Validation",
            "scalability",
            "Contract/profile/runtime present but no positive volume evidence "
            "(expected/peak/profile bytes or measured runtime bytes).",
        )

    # Initial evaluable skeleton; CheckpointEngine evaluates SCALABILITY-* rules
    return Checkpoint(
        checkpoint_id="CP-010",
        name="Scalability Validation",
        category="scalability",
        status=CheckpointStatus.PASS,
        severity=Severity.INFO,
        score=1.0,
        evidence=EvidenceRecord(
            rule_id="CP-010",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            observed={
                "expected_volume_gb": contract.expected_daily_volume_gb if contract else None,
                "peak_volume_gb": contract.peak_daily_volume_gb if contract else None,
                "profile_volume_gb": data_profile.total_gb if data_profile else None,
            },
            expected={"scalability_assessed": True},
            evidence=["Workload scenarios generated across baseline, expected, and peak targets"],
            recommendation="",
            confidence=0.8,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp011_job(job_config: dict[str, Any] | None) -> Checkpoint:
    if not job_config:
        return _unknown("CP-011", "Job Validation", "job", "No job configuration available.")
    status, severity, score = CheckpointStatus.PASS, Severity.INFO, 1.0
    notes = []
    settings = job_config.get("settings", job_config)
    tasks = settings.get("tasks", []) if isinstance(settings, dict) else []
    # Phase 6 (B1): an arbitrary mapping without any of the fields this
    # checkpoint evaluates (tasks, schedule, retry or timeout evidence) is
    # not job-configuration evidence. Reliability-only configs (retries +
    # timeout, as synthesized offline) still proceed to the checks below.
    _schedule = settings.get("schedule") if isinstance(settings, dict) else None
    _has_job_signal = (
        bool(tasks)
        or bool(_schedule)
        or bool(job_config.get("schedule"))
        or "max_retries" in job_config
        or "timeout_seconds" in job_config
        or "retry_count" in job_config
        or "timeout_minutes" in job_config
        or (isinstance(settings, dict) and ("max_retries" in settings or "timeout_seconds" in settings))
    )
    if not _has_job_signal:
        return _unknown(
            "CP-011",
            "Job Validation",
            "job",
            "Job configuration present but no task, schedule, retry, or timeout evidence available.",
        )
    max_retries = int(job_config.get("max_retries", 0) or 0)
    if isinstance(settings, dict):
        max_retries = max(max_retries, int(settings.get("max_retries", 0) or 0))
    if tasks:
        task_retries = [int(t.get("max_retries", 0) or 0) for t in tasks if isinstance(t, dict)]
        if task_retries:
            max_retries = max(max_retries, max(task_retries))

    timeout_sec = int(job_config.get("timeout_seconds", 0) or 0)
    if isinstance(settings, dict) and "timeout_seconds" in settings:
        timeout_sec = max(timeout_sec, int(settings.get("timeout_seconds", 0) or 0))
    if tasks:
        task_timeouts = [
            int(t.get("timeout_seconds", 0) or 0) for t in tasks if isinstance(t, dict)
        ]
        if task_timeouts:
            timeout_sec = max(timeout_sec, max(task_timeouts))

    if timeout_sec <= 0:
        status, severity, score = CheckpointStatus.WARN, Severity.MEDIUM, 0.6
        notes.append("timeout not configured")
    if max_retries < 1:
        status = CheckpointStatus.WARN if status == CheckpointStatus.PASS else status
        severity = Severity.MEDIUM
        score = min(score, 0.7)
        notes.append("no retries configured")
    return Checkpoint(
        checkpoint_id="CP-011",
        name="Job Validation",
        category="job",
        status=status,
        severity=severity,
        score=score,
        evidence=EvidenceRecord(
            rule_id="CP-011",
            status=status,
            severity=severity,
            observed=dict(job_config),
            expected={"max_retries": ">= 1"},
            evidence=["job configuration"],
            recommendation="; ".join(notes),
            confidence=0.7,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp012_incremental(contract: PipelineContract | None) -> Checkpoint:
    cp_id, name, cat = "CP-012", "Incremental Processing Validation", "pipeline"
    if contract is None:
        return _unknown(cp_id, name, cat, "No pipeline contract available.")
    processing = (contract.processing or "").lower()
    expected = contract.expected_daily_volume_gb or 0.0
    # No depends_on: the verdict derives from the contract alone, so a data
    # finding must not mask this FAIL as UNKNOWN.
    if processing in ("full_load", "full-load", "full load") and expected >= 100:
        return Checkpoint(
            checkpoint_id=cp_id,
            name=name,
            category=cat,
            status=CheckpointStatus.FAIL,
            severity=Severity.HIGH,
            score=0.2,
            evidence=EvidenceRecord(
                rule_id=cp_id,
                status=CheckpointStatus.FAIL,
                severity=Severity.HIGH,
                observed={"processing": contract.processing, "expected_daily_volume_gb": expected},
                expected={"processing": "incremental for large volumes"},
                evidence=["pipeline contract processing + volume"],
                recommendation=(
                    "Implement incremental processing (watermark / CDC / merge); "
                    "full reload at this volume is HIGH RISK."
                ),
                confidence=0.8,
                method=AnalysisMethod.METADATA,
            ),
        )
    if processing in ("incremental", "cdc", "streaming", "micro-batch", "micro_batch"):
        return Checkpoint(
            checkpoint_id=cp_id,
            name=name,
            category=cat,
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            score=1.0,
            evidence=EvidenceRecord(
                rule_id=cp_id,
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                observed={"processing": contract.processing},
                expected={"processing": "incremental-family"},
                evidence=["pipeline contract processing"],
                recommendation="",
                confidence=0.8,
                method=AnalysisMethod.METADATA,
            ),
        )
    if processing in ("batch", "micro-batch-schedule", ""):
        return Checkpoint(
            checkpoint_id=cp_id,
            name=name,
            category=cat,
            status=CheckpointStatus.WARN,
            severity=Severity.MEDIUM,
            score=0.6,
            evidence=EvidenceRecord(
                rule_id=cp_id,
                status=CheckpointStatus.WARN,
                severity=Severity.MEDIUM,
                observed={"processing": contract.processing, "expected_daily_volume_gb": expected},
                expected={"processing": "explicit incremental or full_load declaration"},
                evidence=["pipeline contract processing"],
                recommendation=(
                    "Declare the reload strategy explicitly (incremental vs full_load); "
                    "'batch' only describes scheduling."
                ),
                confidence=0.7,
                method=AnalysisMethod.METADATA,
            ),
        )
    return _unknown(cp_id, name, cat, f"Processing strategy unclear: {contract.processing!r}.")


def cp013_error_handling(code_text: str = "") -> Checkpoint:
    if not code_text or not code_text.strip():
        return _unknown(
            "CP-013",
            "Error Handling Validation",
            "reliability",
            "No code available to assess error handling.",
        )
    # Phase 6 (B1): the substrings "try:"/"except" alone (e.g. inside a
    # comment) do not prove exception safety. A PASS requires a parsed
    # try/except handler; textual mention without a parsed handler is
    # insufficient evidence (UNKNOWN), while code with no mention at all
    # remains a genuine negative (WARN).
    _mentions_try = "try:" in code_text and "except" in code_text
    if _mentions_try:
        try:
            _tree = ast.parse(code_text)
            _verified = any(
                isinstance(_node, ast.Try) and _node.handlers for _node in ast.walk(_tree)
            )
        except (SyntaxError, ValueError):
            _verified = False
        if not _verified:
            return _unknown(
                "CP-013",
                "Error Handling Validation",
                "reliability",
                "Code mentions try/except textually but no parsed exception handler "
                "was found; error-handling evidence unavailable.",
            )
    has_try = _mentions_try
    status = CheckpointStatus.PASS if has_try else CheckpointStatus.WARN
    return Checkpoint(
        checkpoint_id="CP-013",
        name="Error Handling Validation",
        category="reliability",
        status=status,
        severity=Severity.INFO if has_try else Severity.MEDIUM,
        score=1.0 if has_try else 0.6,
        evidence=EvidenceRecord(
            rule_id="CP-013",
            status=status,
            severity=Severity.INFO if has_try else Severity.MEDIUM,
            observed={"try_except_found": has_try},
            expected={"error_handling": True},
            evidence=["static code scan"],
            recommendation="" if has_try else "Add try/except with logging around I/O and writes.",
            confidence=0.65,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp014_retry(
    contract: PipelineContract | None, job_config: dict[str, Any] | None = None
) -> Checkpoint:
    retries = None
    if job_config and job_config.get("max_retries") is not None:
        retries = job_config.get("max_retries")
    elif contract is not None:
        retries = contract.reliability.retry_count
    if retries is None:
        return _unknown(
            "CP-014", "Retry Validation", "reliability", "No retry configuration available."
        )
    ok = int(retries) >= 1
    return Checkpoint(
        checkpoint_id="CP-014",
        name="Retry Validation",
        category="reliability",
        status=CheckpointStatus.PASS if ok else CheckpointStatus.FAIL,
        severity=Severity.INFO if ok else Severity.HIGH,
        score=1.0 if ok else 0.3,
        depends_on=["CP-011"],
        evidence=EvidenceRecord(
            rule_id="CP-014",
            status=CheckpointStatus.PASS if ok else CheckpointStatus.FAIL,
            severity=Severity.INFO if ok else Severity.HIGH,
            observed={"retry_count": retries},
            expected={"retry_count": ">= 1"},
            evidence=["job/contract reliability config"],
            recommendation="" if ok else "Configure at least 1 retry with backoff.",
            confidence=0.8,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp015_restartability(
    job_config: dict[str, Any] | None = None, code_text: str = ""
) -> Checkpoint:
    if not job_config and not code_text.strip():
        return _unknown(
            "CP-015",
            "Restartability Validation",
            "reliability",
            "No checkpoint/restart metadata available.",
        )
    text = f"{job_config or {}} {code_text}".lower()
    _nospace = text.replace(" ", "")
    # Phase 6 (B1): the bare word "checkpoint" in prose (e.g. a TODO comment)
    # does not prove checkpoint lifecycle correctness. PASS requires a
    # checkpoint location/restart signal; a bare mention is insufficient
    # evidence (UNKNOWN); no mention at all remains a genuine negative (WARN).
    _strong_ckpt = (
        "checkpointlocation" in _nospace
        or "checkpoint_location" in text
        or "checkpoint_path" in text
        or "checkpoint_dir" in text
        or (("restart" in text or "recover" in text) and "checkpoint" in text)
    )
    if "checkpoint" not in text:
        has_ckpt = False
    elif _strong_ckpt:
        has_ckpt = True
    else:
        return _unknown(
            "CP-015",
            "Restartability Validation",
            "reliability",
            "The word 'checkpoint' appears but no checkpoint location or restart "
            "evidence was found; restartability evidence unavailable.",
        )
    status = CheckpointStatus.PASS if has_ckpt else CheckpointStatus.WARN
    return Checkpoint(
        checkpoint_id="CP-015",
        name="Restartability Validation",
        category="reliability",
        status=status,
        severity=Severity.INFO if has_ckpt else Severity.MEDIUM,
        score=1.0 if has_ckpt else 0.6,
        depends_on=["CP-011"],
        evidence=EvidenceRecord(
            rule_id="CP-015",
            status=status,
            severity=Severity.INFO if has_ckpt else Severity.MEDIUM,
            observed={"checkpointing_found": has_ckpt},
            expected={"checkpointing": True},
            evidence=["job config / code scan"],
            recommendation=(
                "" if has_ckpt else "Configure checkpoint locations for restart-safe processing."
            ),
            confidence=0.6,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp016_idempotency(contract: PipelineContract | None) -> Checkpoint:
    if contract is None:
        return _unknown("CP-016", "Idempotency Validation", "reliability", "No contract available.")
    # Phase 6 (B1): the model default (idempotent=True) is not evidence.
    # PASS/WARN require explicitly provided idempotency evidence; a bare
    # default-constructed reliability section stays UNKNOWN. (Synthesis and
    # loader defaults are out of scope for this phase.)
    _reliability = getattr(contract, "reliability", None)
    if _reliability is None or "idempotent" not in _reliability.model_fields_set:
        return _unknown(
            "CP-016",
            "Idempotency Validation",
            "reliability",
            "No explicit idempotency evidence in the contract reliability section.",
        )
    ok = bool(_reliability.idempotent)
    return Checkpoint(
        checkpoint_id="CP-016",
        name="Idempotency Validation",
        category="reliability",
        status=CheckpointStatus.PASS if ok else CheckpointStatus.WARN,
        severity=Severity.INFO if ok else Severity.MEDIUM,
        score=1.0 if ok else 0.6,
        depends_on=["CP-011"],
        evidence=EvidenceRecord(
            rule_id="CP-016",
            status=CheckpointStatus.PASS if ok else CheckpointStatus.WARN,
            severity=Severity.INFO if ok else Severity.MEDIUM,
            observed={"idempotent": ok},
            expected={"idempotent": True},
            evidence=["contract reliability section"],
            recommendation=(
                "" if ok else "Prove idempotency (merge keys / dedupe) before enabling retries."
            ),
            confidence=0.75,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp019_cost() -> Checkpoint:
    return _unknown(
        "CP-019",
        "Cost Validation",
        "cost",
        "No DBU/pricing information available; cost analysis unavailable.",
    )


def cp020_security(code_text: str = "") -> Checkpoint:
    if not code_text.strip():
        return _unknown(
            "CP-020",
            "Security Validation",
            "security",
            "No code/metadata available for security scan.",
        )
    secret_pattern = r"(?i)(password|passwd|secret|token|api[_-]?key)\s*=\s*['\"][^'\"]+['\"]"
    hits = re.findall(secret_pattern, code_text)
    if hits:
        return Checkpoint(
            checkpoint_id="CP-020",
            name="Security Validation",
            category="security",
            status=CheckpointStatus.FAIL,
            severity=Severity.CRITICAL,
            score=0.0,
            evidence=EvidenceRecord(
                rule_id="CP-020",
                status=CheckpointStatus.FAIL,
                severity=Severity.CRITICAL,
                observed={"hardcoded_secret_patterns": len(hits)},
                expected={"hardcoded_secrets": 0},
                evidence=["static code scan (values masked, never logged)"],
                recommendation=(
                    "Remove hard-coded credentials; use secret scopes / environment variables."
                ),
                confidence=0.9,
                method=AnalysisMethod.METADATA,
            ),
        )
    return Checkpoint(
        checkpoint_id="CP-020",
        name="Security Validation",
        category="security",
        status=CheckpointStatus.PASS,
        severity=Severity.INFO,
        score=1.0,
        evidence=EvidenceRecord(
            rule_id="CP-020",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            observed={"hardcoded_secret_patterns": 0},
            expected={"hardcoded_secrets": 0},
            evidence=["static code scan"],
            recommendation="",
            confidence=0.6,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp021_governance(contract: PipelineContract | None) -> Checkpoint:
    if contract is None:
        return _unknown("CP-021", "Governance Validation", "governance", "No contract available.")
    missing = [f for f in ("owner",) if not getattr(contract, f, "")]
    if missing:
        return Checkpoint(
            checkpoint_id="CP-021",
            name="Governance Validation",
            category="governance",
            status=CheckpointStatus.WARN,
            severity=Severity.MEDIUM,
            score=0.6,
            evidence=EvidenceRecord(
                rule_id="CP-021",
                status=CheckpointStatus.WARN,
                severity=Severity.MEDIUM,
                observed={"owner": contract.owner},
                expected={"owner": "set", "catalog": "unity-catalog"},
                evidence=["pipeline contract"],
                recommendation="Set owner, catalog/schema, tags and documentation.",
                confidence=0.7,
                method=AnalysisMethod.METADATA,
            ),
        )
    # Phase 6 (B1): owner presence alone does not prove governance/catalog
    # completeness. A PASS requires catalog evidence on the target; an owner
    # without any catalog stays UNKNOWN (never a silent PASS).
    _catalog = getattr(getattr(contract, "target", None), "catalog", "") or ""
    if not str(_catalog).strip():
        return _unknown(
            "CP-021",
            "Governance Validation",
            "governance",
            "Owner is set but no Unity Catalog (target catalog) evidence available; "
            "governance completeness unknown.",
        )
    return Checkpoint(
        checkpoint_id="CP-021",
        name="Governance Validation",
        category="governance",
        status=CheckpointStatus.PASS,
        severity=Severity.INFO,
        score=1.0,
        evidence=EvidenceRecord(
            rule_id="CP-021",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            observed={"owner": contract.owner, "environment": contract.environment},
            expected={"owner": "set"},
            evidence=["pipeline contract"],
            recommendation="",
            confidence=0.7,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp022_data_quality(profile: DataProfile | None) -> Checkpoint:
    if profile is None or not profile.has_sufficient_metadata:
        return _unknown(
            "CP-022",
            "Data Quality Validation",
            "data-quality",
            "No data profile available for quality checks.",
        )
    # Phase 6 (B1): volume/file presence alone does not prove data-quality
    # configuration. PASS requires data-shape/quality evidence (records,
    # columns, or quality indicators); a volume-only profile stays UNKNOWN.
    _has_quality_evidence = (
        (profile.record_count or 0) > 0
        or (profile.column_count or 0) > 0
        or bool(profile.schema_columns)
        or bool(profile.null_distribution)
        or bool(profile.duplicate_indicators)
    )
    if not _has_quality_evidence:
        return _unknown(
            "CP-022",
            "Data Quality Validation",
            "data-quality",
            "Profile has volume/file metadata but no data-shape or quality "
            "evidence (records, columns, null/duplicate indicators).",
        )
    return Checkpoint(
        checkpoint_id="CP-022",
        name="Data Quality Validation",
        category="data-quality",
        status=CheckpointStatus.PASS,
        severity=Severity.INFO,
        score=1.0,
        evidence=EvidenceRecord(
            rule_id="CP-022",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            observed={"record_count": profile.record_count, "column_count": profile.column_count},
            expected={"quality_rules": "configured"},
            evidence=["data profile metadata"],
            recommendation="Configure null/duplicate/schema-drift rules for production.",
            confidence=0.6,
            method=profile.analysis_method,
        ),
    )


def cp008_performance(runtime_data: Any | None = None) -> Checkpoint:
    """Skeleton — the engine executes performance rules against runtime_run.

    Phase 6 (B1): a runtime object alone does not prove runtime quality.
    PASS requires measured evidence (positive duration or stage telemetry);
    an object without either stays UNKNOWN.
    """
    if runtime_data is None:
        return _unknown(
            "CP-008",
            "Performance Validation",
            "performance",
            "No runtime execution metrics or event log available for performance validation.",
        )
    _duration: Any = getattr(runtime_data, "duration_seconds", None)
    _stages: Any = getattr(runtime_data, "stages", None)
    if isinstance(runtime_data, dict):
        _duration = runtime_data.get("duration_seconds", _duration)
        _stages = runtime_data.get("stages", _stages)
    try:
        _duration_val = float(_duration) if _duration is not None else 0.0
    except (TypeError, ValueError):
        _duration_val = 0.0
    _has_measured = _duration_val > 0 or (isinstance(_stages, list) and len(_stages) > 0)
    if not _has_measured:
        return _unknown(
            "CP-008",
            "Performance Validation",
            "performance",
            "Runtime evidence present but no measured duration or stage telemetry available.",
        )
    return Checkpoint(
        checkpoint_id="CP-008",
        name="Performance Validation",
        category="performance",
        status=CheckpointStatus.PASS,
        severity=Severity.INFO,
        score=1.0,
        evidence=EvidenceRecord(
            rule_id="CP-008",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            observed={
                "runtime_available": True,
                "duration_seconds": _duration_val,
                "stage_count": len(_stages) if isinstance(_stages, list) else 0,
            },
            expected={"runtime_evidence": True},
            evidence=["runtime execution evidence"],
            recommendation="",
            confidence=0.85,
            method=AnalysisMethod.METADATA,
        ),
    )


def cp023_sla(
    contract: PipelineContract | None = None,
    runtime_data: Any | None = None,
) -> Checkpoint:
    if runtime_data is None:
        return _unknown(
            "CP-023",
            "SLA Validation",
            "sla",
            "No runtime metrics available; SLA comparison requires actual/projected runtimes.",
        )
    sla_min = (
        getattr(contract.sla, "max_runtime_minutes", None) if contract and contract.sla else None
    )
    if sla_min is None and contract and contract.sla:
        sla_min = getattr(contract.sla, "max_duration_minutes", None) or getattr(
            contract.sla, "expected_duration_minutes", None
        )

    dur_sec = getattr(runtime_data, "duration_seconds", 0.0)
    if isinstance(runtime_data, dict):
        dur_sec = float(runtime_data.get("duration_seconds", 0.0) or 0.0)

    if sla_min is not None and sla_min > 0:
        sla_sec = float(sla_min) * 60.0
        if dur_sec > sla_sec:
            return Checkpoint(
                checkpoint_id="CP-023",
                name="SLA Validation",
                category="sla",
                status=CheckpointStatus.FAIL,
                severity=Severity.HIGH,
                score=0.2,
                evidence=EvidenceRecord(
                    rule_id="CP-023",
                    status=CheckpointStatus.FAIL,
                    severity=Severity.HIGH,
                    observed={
                        "actual_duration_seconds": dur_sec,
                        "actual_duration_minutes": dur_sec / 60.0,
                    },
                    expected={"max_duration_minutes": sla_min, "max_duration_seconds": sla_sec},
                    evidence=[
                        f"Execution duration ({dur_sec / 60.0:.1f}m) "
                        f"exceeded contractual SLA ({sla_min}m)"
                    ],
                    recommendation=(
                        "Optimize pipeline runtime to comply with defined service level agreement."
                    ),
                    confidence=0.95,
                    method=AnalysisMethod.METADATA,
                ),
            )
        else:
            return Checkpoint(
                checkpoint_id="CP-023",
                name="SLA Validation",
                category="sla",
                status=CheckpointStatus.PASS,
                severity=Severity.INFO,
                score=1.0,
                evidence=EvidenceRecord(
                    rule_id="CP-023",
                    status=CheckpointStatus.PASS,
                    severity=Severity.INFO,
                    observed={
                        "actual_duration_seconds": dur_sec,
                        "actual_duration_minutes": dur_sec / 60.0,
                    },
                    expected={"max_duration_minutes": sla_min},
                    evidence=[
                        f"Execution duration ({dur_sec / 60.0:.1f}m) "
                        f"within contractual SLA ({sla_min}m)"
                    ],
                    recommendation="",
                    confidence=0.95,
                    method=AnalysisMethod.METADATA,
                ),
            )

    return Checkpoint(
        checkpoint_id="CP-023",
        name="SLA Validation",
        category="sla",
        status=CheckpointStatus.UNKNOWN
        if dur_sec <= 0
        else CheckpointStatus.PASS,
        severity=Severity.INFO,
        score=0.0 if dur_sec <= 0 else 1.0,
        evidence=EvidenceRecord(
            rule_id="CP-023",
            status=CheckpointStatus.UNKNOWN
            if dur_sec <= 0
            else CheckpointStatus.PASS,
            severity=Severity.INFO,
            observed={"actual_duration_seconds": dur_sec},
            expected={"sla_configured": False, "duration_measured": dur_sec > 0},
            evidence=[
                "No runtime duration measured and no contractual SLA defined; "
                "SLA compliance is unknown"
                if dur_sec <= 0
                else "Runtime observed; no contractual SLA defined to compare against"
            ],
            recommendation="Provide runtime execution evidence for SLA validation."
            if dur_sec <= 0
            else "",
            confidence=0.0 if dur_sec <= 0 else 0.8,
            method=AnalysisMethod.UNAVAILABLE if dur_sec <= 0 else AnalysisMethod.METADATA,
        ),
    )


def cp024_readiness() -> Checkpoint:
    cp = _unknown("CP-024", "Production Readiness", "readiness", "Aggregated from all checkpoints.")
    cp.depends_on = [
        "CP-001",
        "CP-003",
        "CP-004",
        "CP-007",
        "CP-008",
        "CP-009",
        "CP-010",
        "CP-011",
        "CP-012",
        "CP-013",
        "CP-014",
        "CP-015",
        "CP-016",
        "CP-019",
        "CP-020",
        "CP-021",
        "CP-022",
        "CP-023",
    ]
    return cp


def build_all_checkpoints(
    contract: PipelineContract | None = None,
    data_profile: DataProfile | None = None,
    code_text: str = "",
    cluster_config: dict[str, Any] | None = None,
    job_config: dict[str, Any] | None = None,
    small_file_threshold_kb: float = 1000.0,
    runtime_data: Any | None = None,
    historical_runs: list[Any] | None = None,
) -> list[Checkpoint]:
    """Build the full Phase-2 checkpoint list in dependency order."""
    cps = [
        cp001_source(contract),
        cp003_schema_drift(contract, data_profile),
        cp007_data(data_profile, small_file_threshold_kb),
        cp004_code(code_text),
        cp008_performance(runtime_data),
        cp009_cluster(cluster_config),
        cp010_scalability(
            contract, data_profile, cluster_config, runtime_data, code_text, historical_runs
        ),
        cp011_job(job_config),
        cp012_incremental(contract),
        cp013_error_handling(code_text),
        cp014_retry(contract, job_config),
        cp015_restartability(job_config, code_text),
        cp016_idempotency(contract),
        cp019_cost(),
        cp020_security(code_text),
        cp021_governance(contract),
        cp022_data_quality(data_profile),
        cp023_sla(contract, runtime_data),
        cp024_readiness(),
    ]
    return cps
