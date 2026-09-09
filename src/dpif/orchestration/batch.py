"""Batch validation orchestrator with per-pipeline isolation and failure isolation."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.cli import _load_code_text, _load_metadata_profile
from dpif.config import get_settings
from dpif.contract.loader import contract_cluster_job, load_contract_file
from dpif.error_handling import ConfigurationError, setup_logging
from dpif.models import CheckpointStatus, Severity
from dpif.orchestration.models import (
    BatchValidationResult,
    PipelineProcessingStatus,
    PipelineSubmission,
    PipelineValidationResult,
)
from dpif.readiness.engine import evaluate_production_readiness
from dpif.rules.engine import load_rules
from dpif.scoring.engine import readiness_label, score_checkpoints

logger = setup_logging()

_SECRET_PATTERNS = [
    re.compile(r"(?i)(password|passwd|token|api_key|secret|credential|auth_key)\s*[:=]\s*['\"]?([^\s'\",;&]+)['\"]?"),
    re.compile(r"(?i)(bearer)\s+([a-zA-Z0-9_\-\.=]+)"),
]


def sanitize_error_message(msg: str) -> str:
    """Sanitize error messages to remove sensitive information such as secrets or tokens."""
    sanitized = msg
    for pattern in _SECRET_PATTERNS:
        def _redact(match: re.Match[str]) -> str:
            if len(match.groups()) >= 2:
                key = match.group(1)
                return f"{key}=[REDACTED]"
            return "[REDACTED]"
        sanitized = pattern.sub(_redact, sanitized)
    return sanitized


def validate_single_pipeline_submission(
    submission: PipelineSubmission,
    environment: str | None = None,
) -> PipelineValidationResult:
    """Validate a single pipeline submission in an isolated context.

    Failure isolation: Any exception during execution is caught and returned
    as a PipelineValidationResult with PROCESSING_ERROR or MISSING_INPUT status.
    """
    # Defensive check for malformed submission object
    if not submission.pipeline_id or not submission.pipeline_id.strip():
        return PipelineValidationResult(
            submission=submission,
            processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
            errors=["Malformed submission: missing required pipeline_id"],
        )

    contract_path = submission.resolved_contract_path or submission.contract_path
    code_path = submission.resolved_code_path or submission.code_path
    metadata_path = submission.resolved_metadata_path or submission.metadata_profile
    runtime_path = submission.resolved_runtime_path or submission.runtime_run
    historical_path = submission.resolved_historical_path or submission.historical_runs

    if not contract_path:
        return PipelineValidationResult(
            submission=submission,
            processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
            errors=["Malformed submission: missing contract_path"],
        )
    if not code_path:
        return PipelineValidationResult(
            submission=submission,
            processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
            errors=["Malformed submission: missing code_path"],
        )

    try:
        if not Path(contract_path).exists():
            return PipelineValidationResult(
                submission=submission,
                processing_status=PipelineProcessingStatus.MISSING_INPUT,
                errors=[f"Contract file not found: {contract_path}"],
            )
        if not Path(code_path).exists():
            return PipelineValidationResult(
                submission=submission,
                processing_status=PipelineProcessingStatus.MISSING_INPUT,
                errors=[f"Code file not found: {code_path}"],
            )

        contract = load_contract_file(contract_path)
        if environment:
            contract.environment = environment

        with open(contract_path, encoding="utf-8") as f:
            raw_contract = yaml.safe_load(f) or {}

        profile = _load_metadata_profile(contract.expected_daily_volume_gb, metadata_path)
        contract.source.data_profile = profile
        code_text, code_filename = _load_code_text(contract_path, raw_contract, code_path)
        cluster_config, job_config = contract_cluster_job(contract)

        load_rules()

        from dpif.code.models import AnalysisContext
        from dpif.code.parser import analyze_source

        src = contract.source
        analysis = analyze_source(code_text, filename=code_filename)
        analysis_ctx = AnalysisContext(
            pipeline_name=contract.pipeline_name,
            source_type=src.type.value,
            source_format=(
                src.format.value if hasattr(src.format, "value") else str(src.format)
            ),
            expected_volume_gb=src.expected_volume_gb,
            peak_volume_gb=src.peak_volume_gb,
            processing_type=contract.processing,
            contract_name=contract.contract_id,
            evidence_source="fixture metadata",
            collection_method=profile.collection_method.value,
            code=analysis,
        )
        rule_context: dict[str, Any] = dict(analysis_ctx.to_rule_context())
        rule_context.update(
            {
                "ingestion_mode": (
                    src.ingestion_mode.value
                    if hasattr(src.ingestion_mode, "value")
                    else str(src.ingestion_mode)
                ),
                "expected_schema": (
                    src.schema_definition.to_dict() if src.schema_definition else None
                ),
                "jdbc": src.jdbc.to_dict() if src.jdbc else None,
                "streaming": src.streaming.to_dict() if src.streaming else None,
                "partitioning": list(src.partitioning),
            }
        )

        runtime_obj = None
        if runtime_path and Path(runtime_path).exists():
            from dpif.runtime.models import RuntimeRun
            from dpif.runtime.normalization import normalize_runtime_payload

            with open(runtime_path, encoding="utf-8") as rf:
                raw_rt = (
                    json.loads(rf.read())
                    if runtime_path.endswith(".json")
                    else yaml.safe_load(rf.read())
                )
            norm_rt = normalize_runtime_payload(raw_rt)
            runtime_obj = (
                norm_rt if isinstance(norm_rt, RuntimeRun) else RuntimeRun(**norm_rt)
            )

        historical_objs: list[Any] = []
        if historical_path and Path(historical_path).exists():
            with open(historical_path, encoding="utf-8") as hf:
                raw_h = (
                    json.loads(hf.read())
                    if historical_path.endswith(".json")
                    else yaml.safe_load(hf.read())
                )
            if isinstance(raw_h, list):
                historical_objs = raw_h

        context: dict[str, Any] = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": src,
            "data_profile": profile,
            "code_snippet": code_text,
            "code_filename": code_filename,
            "code_analysis": analysis,
            "cluster_config": cluster_config,
            "job_config": job_config,
            "assumptions": {"mode": "offline-fixture"},
            "rule_context": rule_context,
            "runtime_run": runtime_obj,
            "runtime_data": runtime_obj,
            "historical_runs": historical_objs,
        }

        engine = CheckpointEngine()
        checkpoints = build_all_checkpoints(
            contract=contract,
            data_profile=profile,
            code_text=code_text,
            cluster_config=cluster_config,
            job_config=job_config,
            small_file_threshold_kb=get_settings().small_file_threshold_kb,
            runtime_data=runtime_obj,
            historical_runs=historical_objs,
        )
        results = engine.run_all_checkpoints(checkpoints, context)
        score, has_blocking = score_checkpoints(results)
        has_fail = any(cp.status == CheckpointStatus.FAIL for cp in results.values())
        has_unknown_or_warn = any(
            cp.status in (CheckpointStatus.UNKNOWN, CheckpointStatus.WARN)
            for cp in results.values()
        )

        _ = readiness_label(score, has_blocking, has_fail, has_unknown_or_warn)

        assessment = evaluate_production_readiness(
            checkpoints=results,
            contract=contract,
            profile=profile,
            cluster_config=cluster_config,
            job_config=job_config,
            runtime_data=runtime_obj,
            historical_runs=historical_objs,
        )

        crit_cnt = 0
        high_cnt = 0
        med_cnt = 0
        low_cnt = 0
        info_cnt = 0

        for cp in results.values():
            for finding in cp.findings:
                if finding.severity == Severity.CRITICAL:
                    crit_cnt += 1
                elif finding.severity == Severity.HIGH:
                    high_cnt += 1
                elif finding.severity == Severity.MEDIUM:
                    med_cnt += 1
                elif finding.severity == Severity.LOW:
                    low_cnt += 1
                else:
                    info_cnt += 1

        checkpoints_summary = {
            k: {
                "checkpoint_id": v.checkpoint_id,
                "name": v.name,
                "status": v.status.value,
                "severity": v.severity.value,
                "score": v.score,
                "findings_count": len(v.findings),
            }
            for k, v in sorted(results.items())
        }

        return PipelineValidationResult(
            submission=submission,
            processing_status=PipelineProcessingStatus.VALIDATION_COMPLETE,
            readiness_status=assessment.status.value,
            quality_score=assessment.quality_score,
            evidence_coverage=assessment.evidence_coverage,
            critical_count=crit_cnt,
            high_count=high_cnt,
            medium_count=med_cnt,
            low_count=low_cnt,
            info_count=info_cnt,
            checkpoints_summary=checkpoints_summary,
            assessment_dict=assessment.to_dict(),
        )

    except ConfigurationError as ce:
        return PipelineValidationResult(
            submission=submission,
            processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
            errors=[sanitize_error_message(f"Configuration error: {ce}")],
        )
    except Exception as e:
        logger.exception("Pipeline validation exception for %s: %s", submission.pipeline_id, e)
        return PipelineValidationResult(
            submission=submission,
            processing_status=PipelineProcessingStatus.PROCESSING_ERROR,
            errors=[sanitize_error_message(f"Processing exception: {e}")],
        )


def run_batch_validation(
    submissions: list[PipelineSubmission],
    invalid_results: list[PipelineValidationResult] | None = None,
    environment: str | None = None,
) -> BatchValidationResult:
    """Orchestrate multi-pipeline validation in isolated execution contexts.

    Preserves original manifest order based on line_number or submission order.
    Rejects duplicate pipeline_ids in submissions list.
    """
    batch_result = BatchValidationResult()

    # Index invalid results by pipeline_id or line_number to preserve order
    invalid_by_pid: dict[str, PipelineValidationResult] = {}
    if invalid_results:
        for inv in invalid_results:
            invalid_by_pid[inv.submission.pipeline_id] = inv

    seen_pipeline_ids: set[str] = set()
    all_results: list[PipelineValidationResult] = []

    for sub in submissions:
        pid = sub.pipeline_id
        if pid in seen_pipeline_ids:
            all_results.append(
                PipelineValidationResult(
                    submission=sub,
                    processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                    errors=[f"Duplicate pipeline_id '{pid}' in batch submission"],
                )
            )
            continue

        if pid:
            seen_pipeline_ids.add(pid)

        if pid in invalid_by_pid:
            all_results.append(invalid_by_pid.pop(pid))
            continue

        try:
            res = validate_single_pipeline_submission(sub, environment=environment)
        except Exception as e:
            logger.exception(
                "Unhandled exception validating pipeline %s: %s", sub.pipeline_id, e
            )
            res = PipelineValidationResult(
                submission=sub,
                processing_status=PipelineProcessingStatus.PROCESSING_ERROR,
                errors=[sanitize_error_message(f"Unhandled exception: {e}")],
            )
        all_results.append(res)

    # Any remaining invalid_results that were not part of submissions list
    if invalid_by_pid:
        all_results.extend(invalid_by_pid.values())

    # Sort all results by line_number if set on submissions
    all_results.sort(
        key=lambda r: (
            r.submission.line_number if r.submission.line_number is not None else float("inf")
        )
    )

    batch_result.pipeline_results = all_results
    batch_result.recompute_summaries()
    return batch_result


def export_batch_reports(
    batch_result: BatchValidationResult,
    output_dir: Path | str,
) -> tuple[Path, Path]:
    """Export machine-readable batch_report.json and batch_report.csv.

    Returns:
        tuple of (json_path, csv_path)
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    json_file = out_path / "batch_report.json"
    csv_file = out_path / "batch_report.csv"

    # Export JSON
    with json_file.open("w", encoding="utf-8") as f_json:
        json.dump(batch_result.to_dict(), f_json, indent=2)

    # Export CSV
    import csv

    headers = [
        "pipeline_id",
        "developer",
        "processing_status",
        "readiness_status",
        "quality_score",
        "evidence_coverage",
        "critical_count",
        "high_count",
        "medium_count",
        "low_count",
        "info_count",
        "errors",
    ]

    with csv_file.open("w", encoding="utf-8", newline="") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=headers)
        writer.writeheader()
        for res in batch_result.pipeline_results:
            writer.writerow(
                {
                    "pipeline_id": res.submission.pipeline_id,
                    "developer": res.submission.developer,
                    "processing_status": res.processing_status.value,
                    "readiness_status": res.readiness_status or "UNKNOWN",
                    "quality_score": (
                        f"{res.quality_score:.1f}" if res.quality_score is not None else ""
                    ),
                    "evidence_coverage": (
                        f"{res.evidence_coverage:.1f}" if res.evidence_coverage is not None else ""
                    ),
                    "critical_count": res.critical_count,
                    "high_count": res.high_count,
                    "medium_count": res.medium_count,
                    "low_count": res.low_count,
                    "info_count": res.info_count,
                    "errors": "; ".join(res.errors),
                }
            )

    return json_file, csv_file
