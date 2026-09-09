"""Unit tests for batch result aggregation and serialization."""

from dpif.orchestration.models import (
    BatchValidationResult,
    PipelineProcessingStatus,
    PipelineSubmission,
    PipelineValidationResult,
)


def test_batch_results_aggregation_and_dict():
    sub1 = PipelineSubmission(
        pipeline_id="P001",
        developer="DevA",
        contract_path="contracts/p1.yaml",
        code_path="code/p1.py",
    )
    sub2 = PipelineSubmission(
        pipeline_id="P002",
        developer="DevB",
        contract_path="contracts/p2.yaml",
        code_path="code/p2.py",
    )
    sub3 = PipelineSubmission(
        pipeline_id="P003",
        developer="DevC",
        contract_path="contracts/p3.yaml",
        code_path="code/p3.py",
    )

    r1 = PipelineValidationResult(
        submission=sub1,
        processing_status=PipelineProcessingStatus.VALIDATION_COMPLETE,
        readiness_status="PRODUCTION_READY",
        quality_score=95.0,
        evidence_coverage=100.0,
        critical_count=0,
        high_count=0,
        medium_count=1,
        low_count=2,
    )
    r2 = PipelineValidationResult(
        submission=sub2,
        processing_status=PipelineProcessingStatus.VALIDATION_COMPLETE,
        readiness_status="NOT_PRODUCTION_READY",
        quality_score=45.0,
        evidence_coverage=60.0,
        critical_count=2,
        high_count=3,
        medium_count=0,
        low_count=0,
    )
    r3 = PipelineValidationResult(
        submission=sub3,
        processing_status=PipelineProcessingStatus.MISSING_INPUT,
        errors=["Missing contract file"],
    )

    batch = BatchValidationResult(pipeline_results=[r1, r2, r3])
    d = batch.to_dict()

    assert d["summary"]["total_submissions"] == 3
    assert d["summary"]["validated"] == 2
    assert d["summary"]["missing_input"] == 1
    assert d["summary"]["invalid_submissions"] == 0
    assert d["summary"]["processing_errors"] == 0

    assert d["summary"]["readiness"]["production_ready"] == 1
    assert d["summary"]["readiness"]["not_production_ready"] == 1

    assert d["summary"]["finding_counts"]["critical"] == 2
    assert d["summary"]["finding_counts"]["high"] == 3
    assert d["summary"]["finding_counts"]["medium"] == 1
    assert d["summary"]["finding_counts"]["low"] == 2

    assert len(d["pipelines"]) == 3
    assert d["pipelines"][0]["pipeline_id"] == "P001"
    assert d["pipelines"][2]["errors"] == ["Missing contract file"]
