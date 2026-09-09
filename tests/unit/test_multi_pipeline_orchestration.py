"""Direct M2 multi-pipeline orchestration and isolation tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from dpif.orchestration.batch import (
    run_batch_validation,
    validate_single_pipeline_submission,
)
from dpif.orchestration.models import (
    PipelineProcessingStatus,
    PipelineSubmission,
    PipelineValidationResult,
)


def _create_pipeline_files(
    tmp_path: Path,
    pid: str,
    contract_name: str | None = None,
    code_content: str | None = None,
) -> tuple[Path, Path]:
    """Helper to create minimal contract and code files for a pipeline."""
    pipe_dir = tmp_path / pid
    pipe_dir.mkdir(parents=True, exist_ok=True)
    c_path = pipe_dir / "contract.yaml"
    code_path = pipe_dir / "code.py"

    c_name = contract_name or f"contract_{pid}"
    c_content = f"""
contract_id: {c_name}
pipeline_name: {c_name}
version: "1.0"
owner: owner@test.com
description: Pipeline contract for {pid}
environment: dev
processing:
  type: batch
expected_daily_volume_gb: 10.0
source:
  type: event_hub
  format: json
  expected_volume_gb: 10.0
  peak_volume_gb: 15.0
  partitioning: [date]
target:
  type: delta_lake
  table: target_table_{pid}
  location: dbfs:/mnt/target
"""
    c_path.write_text(c_content, encoding="utf-8")
    code_path.write_text(code_content or f"print('Executing {pid}')", encoding="utf-8")
    return c_path, code_path


def test_m2_direct_orchestration_pipeline_isolation(tmp_path: Path):
    """Requirement 2: Direct pipeline isolation test comparing P001 and P002."""
    c1, code1 = _create_pipeline_files(tmp_path, "P001", "contract_p001", "print('P001 code')")
    c2, code2 = _create_pipeline_files(tmp_path, "P002", "contract_p002", "df.collect()")

    sub1 = PipelineSubmission(
        pipeline_id="P001",
        developer="DevAlpha",
        contract_path=str(c1),
        code_path=str(code1),
        line_number=2,
        resolved_contract_path=str(c1.resolve()),
        resolved_code_path=str(code1.resolve()),
    )
    sub2 = PipelineSubmission(
        pipeline_id="P002",
        developer="DevBeta",
        contract_path=str(c2),
        code_path=str(code2),
        line_number=3,
        resolved_contract_path=str(c2.resolve()),
        resolved_code_path=str(code2.resolve()),
    )

    batch_result = run_batch_validation([sub1, sub2])

    assert len(batch_result.pipeline_results) == 2
    res1, res2 = batch_result.pipeline_results[0], batch_result.pipeline_results[1]

    # Pipeline ID and Developer preservation
    assert res1.submission.pipeline_id == "P001"
    assert res2.submission.pipeline_id == "P002"
    assert res1.submission.developer == "DevAlpha"
    assert res2.submission.developer == "DevBeta"

    # Statuses
    assert res1.processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert res2.processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE

    # Scores and readiness isolation (P002 has df.collect finding, P001 doesn't)
    assert res1.quality_score is not None
    assert res2.quality_score is not None
    assert res1.checkpoints_summary != res2.checkpoints_summary

    # Assessment dict instance isolation
    assert res1.assessment_dict is not None
    assert res2.assessment_dict is not None
    assert res1.assessment_dict is not res2.assessment_dict
    assert res1.checkpoints_summary is not res2.checkpoints_summary


def test_m2_direct_failure_isolation(tmp_path: Path):
    """Requirement 3: P001 (valid), P002 (missing contract), P003 (valid)."""
    c1, code1 = _create_pipeline_files(tmp_path, "P001")
    c3, code3 = _create_pipeline_files(tmp_path, "P003")

    sub1 = PipelineSubmission(
        pipeline_id="P001",
        developer="Dev1",
        contract_path=str(c1),
        code_path=str(code1),
        line_number=2,
        resolved_contract_path=str(c1.resolve()),
        resolved_code_path=str(code1.resolve()),
    )
    sub2 = PipelineSubmission(
        pipeline_id="P002",
        developer="Dev2",
        contract_path="missing_contract.yaml",
        code_path="missing_code.py",
        line_number=3,
        resolved_contract_path=str(tmp_path / "missing_contract.yaml"),
        resolved_code_path=str(tmp_path / "missing_code.py"),
    )
    sub3 = PipelineSubmission(
        pipeline_id="P003",
        developer="Dev3",
        contract_path=str(c3),
        code_path=str(code3),
        line_number=4,
        resolved_contract_path=str(c3.resolve()),
        resolved_code_path=str(code3.resolve()),
    )

    batch_result = run_batch_validation([sub1, sub2, sub3])

    assert len(batch_result.pipeline_results) == 3
    res_map = {r.submission.pipeline_id: r for r in batch_result.pipeline_results}

    assert res_map["P001"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert res_map["P002"].processing_status == PipelineProcessingStatus.MISSING_INPUT
    assert res_map["P003"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE


def test_m2_processing_error_isolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Requirement 4: Validator runtime error isolation inside run_batch_validation."""
    c1, code1 = _create_pipeline_files(tmp_path, "P001")
    c3, code3 = _create_pipeline_files(tmp_path, "P003")

    sub1 = PipelineSubmission(
        pipeline_id="P001",
        developer="Dev1",
        contract_path=str(c1),
        code_path=str(code1),
        line_number=2,
        resolved_contract_path=str(c1.resolve()),
        resolved_code_path=str(code1.resolve()),
    )
    sub2 = PipelineSubmission(
        pipeline_id="P002",
        developer="Dev2",
        contract_path=str(c1),
        code_path=str(code1),
        line_number=3,
        resolved_contract_path=str(c1.resolve()),
        resolved_code_path=str(code1.resolve()),
    )
    sub3 = PipelineSubmission(
        pipeline_id="P003",
        developer="Dev3",
        contract_path=str(c3),
        code_path=str(code3),
        line_number=4,
        resolved_contract_path=str(c3.resolve()),
        resolved_code_path=str(code3.resolve()),
    )

    orig_validate = validate_single_pipeline_submission

    def mock_validate(sub, environment=None):
        if sub.pipeline_id == "P002":
            raise RuntimeError("Catastrophic engine crash on P002!")
        return orig_validate(sub, environment=environment)

    monkeypatch.setattr(
        "dpif.orchestration.batch.validate_single_pipeline_submission",
        mock_validate,
    )

    batch_result = run_batch_validation([sub1, sub2, sub3])

    assert len(batch_result.pipeline_results) == 3
    res_map = {r.submission.pipeline_id: r for r in batch_result.pipeline_results}

    assert res_map["P001"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert res_map["P002"].processing_status == PipelineProcessingStatus.PROCESSING_ERROR
    assert "Catastrophic engine crash" in res_map["P002"].errors[0]
    assert res_map["P003"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE


def test_m2_readiness_vs_processing_status_assertions(tmp_path: Path):
    """Requirement 5: Explicit assertions on readiness vs processing status semantics."""
    sub_err = PipelineSubmission(
        pipeline_id="P_ERR", developer="D1", contract_path="a", code_path="b"
    )
    res_err = PipelineValidationResult(
        submission=sub_err,
        processing_status=PipelineProcessingStatus.PROCESSING_ERROR,
        errors=["Engine failure"],
    )

    sub_miss = PipelineSubmission(
        pipeline_id="P_MISS", developer="D2", contract_path="a", code_path="b"
    )
    res_miss = PipelineValidationResult(
        submission=sub_miss,
        processing_status=PipelineProcessingStatus.MISSING_INPUT,
        errors=["File not found"],
    )

    # Prove PROCESSING_ERROR cannot be reported as PRODUCTION_READY
    assert res_err.readiness_status is None
    assert res_err.readiness_status != "PRODUCTION_READY"

    # Prove MISSING_INPUT does not receive a fabricated readiness status or quality score
    assert res_miss.readiness_status is None
    assert res_miss.quality_score is None
    assert res_miss.evidence_coverage is None


def test_m2_deterministic_ordering_with_invalid_results(tmp_path: Path):
    """Requirement 6: Manifest order preservation with mixed valid and invalid submissions."""
    c1, code1 = _create_pipeline_files(tmp_path, "P001")
    c3, code3 = _create_pipeline_files(tmp_path, "P003")

    sub1 = PipelineSubmission(
        pipeline_id="P001",
        developer="Dev1",
        contract_path=str(c1),
        code_path=str(code1),
        line_number=2,
        resolved_contract_path=str(c1.resolve()),
        resolved_code_path=str(code1.resolve()),
    )
    sub2 = PipelineSubmission(
        pipeline_id="P002",
        developer="Dev2",
        contract_path="bad.yaml",
        code_path="bad.py",
        line_number=3,
    )
    sub3 = PipelineSubmission(
        pipeline_id="P003",
        developer="Dev3",
        contract_path=str(c3),
        code_path=str(code3),
        line_number=4,
        resolved_contract_path=str(c3.resolve()),
        resolved_code_path=str(code3.resolve()),
    )

    invalid_p002 = PipelineValidationResult(
        submission=sub2,
        processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
        errors=["Invalid CSV row"],
    )

    batch_result = run_batch_validation([sub1, sub3], invalid_results=[invalid_p002])

    ordered_ids = [r.submission.pipeline_id for r in batch_result.pipeline_results]
    assert ordered_ids == ["P001", "P002", "P003"]


def test_m2_10_pipeline_mixed_batch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Requirement 7: 10-pipeline test mixing valid, missing, invalid, processing_error."""
    c_valid, code_valid = _create_pipeline_files(tmp_path, "shared_valid")

    submissions: list[PipelineSubmission] = []
    invalid_results: list[PipelineValidationResult] = []

    for i in range(1, 11):
        pid = f"P{i:03d}"
        line_num = i + 1

        if i == 4:  # Invalid submission (from CSV loader phase)
            sub_inv = PipelineSubmission(
                pipeline_id=pid,
                developer=f"Dev{i}",
                contract_path="bad.yaml",
                code_path="bad.py",
                line_number=line_num,
            )
            invalid_results.append(
                PipelineValidationResult(
                    submission=sub_inv,
                    processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
                    errors=["Malformed CSV row"],
                )
            )
        elif i == 7:  # Missing input file
            sub_miss = PipelineSubmission(
                pipeline_id=pid,
                developer=f"Dev{i}",
                contract_path="nonexistent.yaml",
                code_path="nonexistent.py",
                line_number=line_num,
                resolved_contract_path=str(tmp_path / "nonexistent.yaml"),
                resolved_code_path=str(tmp_path / "nonexistent.py"),
            )
            submissions.append(sub_miss)
        else:
            sub = PipelineSubmission(
                pipeline_id=pid,
                developer=f"Dev{i}",
                contract_path=str(c_valid),
                code_path=str(code_valid),
                line_number=line_num,
                resolved_contract_path=str(c_valid.resolve()),
                resolved_code_path=str(code_valid.resolve()),
            )
            submissions.append(sub)

    # Monkeypatch P009 to raise a processing error exception
    orig_val = validate_single_pipeline_submission

    def mock_val(sub, environment=None):
        if sub.pipeline_id == "P009":
            raise ValueError("Engine exception for P009")
        return orig_val(sub, environment=environment)

    monkeypatch.setattr(
        "dpif.orchestration.batch.validate_single_pipeline_submission",
        mock_val,
    )

    batch_res = run_batch_validation(submissions, invalid_results=invalid_results)

    # Assertions
    assert len(batch_res.pipeline_results) == 10
    assert batch_res.total_submissions == 10

    result_ids = [r.submission.pipeline_id for r in batch_res.pipeline_results]
    expected_ids = [f"P{i:03d}" for i in range(1, 11)]
    assert result_ids == expected_ids  # Deterministic order preservation

    statuses = {r.submission.pipeline_id: r.processing_status for r in batch_res.pipeline_results}
    assert statuses["P004"] == PipelineProcessingStatus.INVALID_SUBMISSION
    assert statuses["P007"] == PipelineProcessingStatus.MISSING_INPUT
    assert statuses["P009"] == PipelineProcessingStatus.PROCESSING_ERROR
    assert statuses["P001"] == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert statuses["P010"] == PipelineProcessingStatus.VALIDATION_COMPLETE


def test_m2_100_pipeline_scalability(tmp_path: Path):
    """Requirement 8: Lightweight scalability orchestration test (20 pipelines)."""
    c_shared, code_shared = _create_pipeline_files(tmp_path, "shared_100")

    subs = [
        PipelineSubmission(
            pipeline_id=f"P{i:03d}",
            developer=f"Developer_{i}",
            contract_path=str(c_shared),
            code_path=str(code_shared),
            line_number=i + 2,
            resolved_contract_path=str(c_shared.resolve()),
            resolved_code_path=str(code_shared.resolve()),
        )
        for i in range(20)
    ]

    batch_result = run_batch_validation(subs)

    assert batch_result.total_submissions == 20
    assert batch_result.validated_count == 20
    assert len(batch_result.pipeline_results) == 20

    # Ensure unique pipeline IDs and ordering
    res_ids = [r.submission.pipeline_id for r in batch_result.pipeline_results]
    assert len(set(res_ids)) == 20
    assert res_ids == [f"P{i:03d}" for i in range(20)]


def test_m2_empty_batch():
    """Requirement 9: Empty batch handling."""
    empty_result = run_batch_validation([])
    assert empty_result.total_submissions == 0
    assert len(empty_result.pipeline_results) == 0
    assert empty_result.validated_count == 0
    assert empty_result.invalid_submission_count == 0
    assert empty_result.missing_input_count == 0
    assert empty_result.processing_error_count == 0


def test_m2_all_invalid_batch(tmp_path: Path):
    """Requirement 10: Batch where every pipeline is missing input or invalid."""
    subs = [
        PipelineSubmission(
            pipeline_id=f"P_BAD_{i}",
            developer=f"Dev_{i}",
            contract_path="missing.yaml",
            code_path="missing.py",
            line_number=i + 2,
            resolved_contract_path=str(tmp_path / "missing.yaml"),
            resolved_code_path=str(tmp_path / "missing.py"),
        )
        for i in range(5)
    ]

    batch_res = run_batch_validation(subs)

    assert batch_res.total_submissions == 5
    assert batch_res.validated_count == 0
    assert len(batch_res.pipeline_results) == 5

    for res in batch_res.pipeline_results:
        assert res.processing_status == PipelineProcessingStatus.MISSING_INPUT
        assert res.readiness_status is None
        assert res.quality_score is None
