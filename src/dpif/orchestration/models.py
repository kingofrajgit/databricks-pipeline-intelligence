"""Data models for enterprise multi-pipeline batch validation."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class PipelineProcessingStatus(StrEnum):
    """Processing status of a pipeline submission in a batch run."""

    VALIDATION_COMPLETE = "VALIDATION_COMPLETE"
    INVALID_SUBMISSION = "INVALID_SUBMISSION"
    MISSING_INPUT = "MISSING_INPUT"
    PROCESSING_ERROR = "PROCESSING_ERROR"


class PipelineSubmission(BaseModel):
    """Manifest submission entry for a single pipeline."""

    pipeline_id: str
    developer: str
    contract_path: str
    code_path: str
    metadata_profile: str | None = None
    runtime_run: str | None = None
    historical_runs: str | None = None
    line_number: int | None = None

    # Resolved absolute or relative paths after safe validation
    resolved_contract_path: str | None = None
    resolved_code_path: str | None = None
    resolved_metadata_path: str | None = None
    resolved_runtime_path: str | None = None
    resolved_historical_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "developer": self.developer,
            "contract_path": self.contract_path,
            "code_path": self.code_path,
            "metadata_profile": self.metadata_profile,
            "runtime_run": self.runtime_run,
            "historical_runs": self.historical_runs,
            "line_number": self.line_number,
        }


class PipelineValidationResult(BaseModel):
    """Validation result for an individual pipeline submission."""

    submission: PipelineSubmission
    processing_status: PipelineProcessingStatus
    readiness_status: str | None = None
    quality_score: float | None = None
    evidence_coverage: float | None = None
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    info_count: int = 0
    errors: list[str] = Field(default_factory=list)
    checkpoints_summary: dict[str, Any] = Field(default_factory=dict)
    assessment_dict: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        res: dict[str, Any] = {
            "pipeline_id": self.submission.pipeline_id,
            "developer": self.submission.developer,
            "processing_status": self.processing_status.value,
            "readiness_status": self.readiness_status or "UNKNOWN",
            "quality_score": self.quality_score,
            "evidence_coverage": self.evidence_coverage,
            "finding_summary": {
                "critical": self.critical_count,
                "high": self.high_count,
                "medium": self.medium_count,
                "low": self.low_count,
                "info": self.info_count,
            },
            "submission": self.submission.to_dict(),
        }
        if self.errors:
            res["errors"] = self.errors
        if self.checkpoints_summary:
            res["checkpoints"] = self.checkpoints_summary
        if self.assessment_dict:
            res["assessment"] = self.assessment_dict
        return res


class BatchValidationResult(BaseModel):
    """Consolidated validation result for a batch execution of multiple pipelines."""

    batch_id: str = Field(
        default_factory=lambda: f"batch_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    )
    timestamp: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())

    total_submissions: int = 0
    validated_count: int = 0
    invalid_submission_count: int = 0
    missing_input_count: int = 0
    processing_error_count: int = 0

    production_ready_count: int = 0
    production_ready_with_warnings_count: int = 0
    not_production_ready_count: int = 0
    insufficient_evidence_count: int = 0

    total_critical_findings: int = 0
    total_high_findings: int = 0
    total_medium_findings: int = 0
    total_low_findings: int = 0

    pipeline_results: list[PipelineValidationResult] = Field(default_factory=list)

    def recompute_summaries(self) -> None:
        """Recompute summary counters from pipeline_results."""
        self.total_submissions = len(self.pipeline_results)
        self.validated_count = sum(
            1
            for r in self.pipeline_results
            if r.processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
        )
        self.invalid_submission_count = sum(
            1
            for r in self.pipeline_results
            if r.processing_status == PipelineProcessingStatus.INVALID_SUBMISSION
        )
        self.missing_input_count = sum(
            1
            for r in self.pipeline_results
            if r.processing_status == PipelineProcessingStatus.MISSING_INPUT
        )
        self.processing_error_count = sum(
            1
            for r in self.pipeline_results
            if r.processing_status == PipelineProcessingStatus.PROCESSING_ERROR
        )

        self.production_ready_count = sum(
            1 for r in self.pipeline_results if r.readiness_status == "PRODUCTION_READY"
        )
        self.production_ready_with_warnings_count = sum(
            1
            for r in self.pipeline_results
            if r.readiness_status == "PRODUCTION_READY_WITH_WARNINGS"
        )
        self.not_production_ready_count = sum(
            1 for r in self.pipeline_results if r.readiness_status == "NOT_PRODUCTION_READY"
        )
        self.insufficient_evidence_count = sum(
            1 for r in self.pipeline_results if r.readiness_status == "INSUFFICIENT_EVIDENCE"
        )

        self.total_critical_findings = sum(r.critical_count for r in self.pipeline_results)
        self.total_high_findings = sum(r.high_count for r in self.pipeline_results)
        self.total_medium_findings = sum(r.medium_count for r in self.pipeline_results)
        self.total_low_findings = sum(r.low_count for r in self.pipeline_results)

    def to_dict(self) -> dict[str, Any]:
        self.recompute_summaries()
        return {
            "batch_id": self.batch_id,
            "timestamp": self.timestamp,
            "summary": {
                "total_submissions": self.total_submissions,
                "validated": self.validated_count,
                "invalid_submissions": self.invalid_submission_count,
                "missing_input": self.missing_input_count,
                "processing_errors": self.processing_error_count,
                "readiness": {
                    "production_ready": self.production_ready_count,
                    "production_ready_with_warnings": self.production_ready_with_warnings_count,
                    "not_production_ready": self.not_production_ready_count,
                    "insufficient_evidence": self.insufficient_evidence_count,
                },
                "finding_counts": {
                    "critical": self.total_critical_findings,
                    "high": self.total_high_findings,
                    "medium": self.total_medium_findings,
                    "low": self.total_low_findings,
                },
            },
            "pipelines": [r.to_dict() for r in self.pipeline_results],
        }
