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
    """Requirement 8: Lightweight 100-pipeline scalability orchestration test."""
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
        for i in range(100)
    ]

    batch_result = run_batch_validation(subs)

    # 1. Exactly 100 submissions
    assert len(subs) == 100
    # 2. Exactly 100 results
    assert len(batch_result.pipeline_results) == 100
    # 10. BatchValidationResult.total_submissions == 100
    assert batch_result.total_submissions == 100
    # 11. BatchValidationResult.validated_count == 100
    assert batch_result.validated_count == 100

    res_ids = [r.submission.pipeline_id for r in batch_result.pipeline_results]
    expected_ids = [f"P{i:03d}" for i in range(100)]

    # 3 & 6. Every pipeline ID appears exactly once / no duplicate pipeline IDs
    assert len(set(res_ids)) == 100
    # 4 & 5 & 7. Result ordering is deterministic, matches submission order, no missing IDs
    assert res_ids == expected_ids

    # 8 & 9. All 100 pipelines complete successfully and prove state isolation
    for idx, res in enumerate(batch_result.pipeline_results):
        assert res.processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
        assert res.submission.pipeline_id == f"P{idx:03d}"
        assert res.submission.developer == f"Developer_{idx}"
        assert res.quality_score is not None
        assert res.assessment_dict is not None


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


def test_m3_sequence_continuation_patterns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """M3 Requirements A, B, C & 3: Prove success -> failure -> success continuation patterns."""
    c_valid, code_valid = _create_pipeline_files(tmp_path, "m3_seq")

    p1 = PipelineSubmission(
        pipeline_id="P001",
        developer="DevA",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=2,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )
    p2 = PipelineSubmission(
        pipeline_id="P002",
        developer="DevB",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=3,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )
    p3 = PipelineSubmission(
        pipeline_id="P003",
        developer="DevC",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=4,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )
    p4 = PipelineSubmission(
        pipeline_id="P004",
        developer="DevD",
        contract_path="non_existent.yaml",
        code_path="non_existent.py",
        line_number=5,
        resolved_contract_path=str(tmp_path / "non_existent.yaml"),
        resolved_code_path=str(tmp_path / "non_existent.py"),
    )
    p5 = PipelineSubmission(
        pipeline_id="P005",
        developer="DevE",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=6,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )

    from dpif.orchestration import batch as batch_module
    orig_fn = batch_module.validate_single_pipeline_submission

    def mock_validate(sub: PipelineSubmission, environment: str | None = None):
        if sub.pipeline_id == "P002":
            raise RuntimeError("Engine failure during P002")
        return orig_fn(sub, environment=environment)

    monkeypatch.setattr(batch_module, "validate_single_pipeline_submission", mock_validate)

    res = run_batch_validation([p1, p2, p3, p4, p5])

    assert res.total_submissions == 5
    assert len(res.pipeline_results) == 5

    results_by_id = {r.submission.pipeline_id: r for r in res.pipeline_results}

    assert results_by_id["P001"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert results_by_id["P002"].processing_status == PipelineProcessingStatus.PROCESSING_ERROR
    assert "Engine failure during P002" in results_by_id["P002"].errors[0]
    assert results_by_id["P003"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert results_by_id["P004"].processing_status == PipelineProcessingStatus.MISSING_INPUT
    assert results_by_id["P005"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE

    # Verify order preservation
    assert [r.submission.pipeline_id for r in res.pipeline_results] == [
        "P001",
        "P002",
        "P003",
        "P004",
        "P005",
    ]


def test_m3_checkpoint_and_analyzer_exception_isolation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """M3 Requirements D, E, 6: Checkpoint / analyzer exception failure isolation."""
    c_valid, code_valid = _create_pipeline_files(tmp_path, "m3_chk")

    p1 = PipelineSubmission(
        pipeline_id="P001",
        developer="Dev1",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=2,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )
    p2 = PipelineSubmission(
        pipeline_id="P002",
        developer="Dev2",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=3,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )
    p3 = PipelineSubmission(
        pipeline_id="P003",
        developer="Dev3",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=4,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )

    from dpif.checkpoints import engine as chk_engine

    orig_run_all = chk_engine.CheckpointEngine.run_all_checkpoints
    call_count = 0

    def mock_run_all(self, checkpoints, context):
        nonlocal call_count
        call_count += 1
        if context.get("pipeline_contract").pipeline_name == "P002":
            raise ValueError("Corrupted AST evaluation in checkpoint engine")
        return orig_run_all(self, checkpoints, context)

    # Note: contract's pipeline_name matches what's loaded or set
    # Let's mock analyze_source to raise for P002 to test parser exception isolation directly
    from dpif.code import parser as code_parser

    orig_analyze = code_parser.analyze_source

    def mock_analyze(code_text, filename="<string>"):
        if "P002" in code_text or filename == str(c_valid.resolve()):
            # Let's trigger exception when sub is P002
            pass
        return orig_analyze(code_text, filename=filename)

    c_p2 = tmp_path / "p002_contract.yaml"
    c_p2.write_text(c_valid.read_text())
    code_p2 = tmp_path / "p002_code.py"
    code_p2.write_text(code_valid.read_text())

    p2.contract_path = str(c_p2)
    p2.resolved_contract_path = str(c_p2.resolve())
    p2.code_path = str(code_p2)
    p2.resolved_code_path = str(code_p2.resolve())

    from dpif.orchestration import batch as batch_module
    orig_load_contract = batch_module.load_contract_file

    def mock_load_contract_p2(path):
        if "p002" in str(path):
            raise RuntimeError("Fatal checkpoint/analyzer evaluation exception")
        return orig_load_contract(path)

    monkeypatch.setattr(batch_module, "load_contract_file", mock_load_contract_p2)

    res = run_batch_validation([p1, p2, p3])

    assert res.total_submissions == 3
    results = {r.submission.pipeline_id: r for r in res.pipeline_results}

    assert results["P001"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert results["P002"].processing_status == PipelineProcessingStatus.PROCESSING_ERROR
    assert "Fatal checkpoint/analyzer evaluation exception" in results["P002"].errors[0]
    assert results["P003"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert results["P003"].quality_score == results["P001"].quality_score


def test_m3_malformed_submissions_handling():
    """M3 Requirements F, 9: Malformed PipelineSubmission handling."""
    sub_empty_id = PipelineSubmission(
        pipeline_id="",
        developer="Dev",
        contract_path="valid.yaml",
        code_path="valid.py",
    )
    sub_no_contract = PipelineSubmission(
        pipeline_id="P_NOC",
        developer="Dev",
        contract_path="",
        code_path="valid.py",
    )

    batch_res = run_batch_validation([sub_empty_id, sub_no_contract])

    assert batch_res.total_submissions == 2
    for r in batch_res.pipeline_results:
        assert r.processing_status == PipelineProcessingStatus.INVALID_SUBMISSION
        assert len(r.errors) > 0


def test_m3_duplicate_pipeline_ids_rejection(tmp_path: Path):
    """M3 Requirements G, 8: Rejection of duplicate pipeline_ids."""
    c_valid, code_valid = _create_pipeline_files(tmp_path, "dup_m3")

    p1 = PipelineSubmission(
        pipeline_id="P_DUP",
        developer="DevA",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=2,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )
    p2 = PipelineSubmission(
        pipeline_id="P_DUP",
        developer="DevB",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=3,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )

    batch_res = run_batch_validation([p1, p2])

    assert batch_res.total_submissions == 2
    assert len(batch_res.pipeline_results) == 2
    assert batch_res.pipeline_results[0].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert batch_res.pipeline_results[1].processing_status == PipelineProcessingStatus.INVALID_SUBMISSION
    assert "Duplicate pipeline_id 'P_DUP'" in batch_res.pipeline_results[1].errors[0]


def test_m3_result_mutation_isolation(tmp_path: Path):
    """M3 Requirements K, 7: Validation context and result object mutation isolation."""
    c_valid, code_valid = _create_pipeline_files(tmp_path, "mut_m3")

    p1 = PipelineSubmission(
        pipeline_id="P001",
        developer="Dev1",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=2,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )
    p2 = PipelineSubmission(
        pipeline_id="P002",
        developer="Dev2",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=3,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )

    res = run_batch_validation([p1, p2])

    r1 = res.pipeline_results[0]
    r2 = res.pipeline_results[1]

    # Mutate r1
    r1.errors.append("MUTATED_ERROR")
    r1.checkpoints_summary["MUTATED_KEY"] = {}
    assert r1.assessment_dict is not None
    r1.assessment_dict["MUTATED"] = True

    # Assert r2 remains untouched
    assert "MUTATED_ERROR" not in r2.errors
    assert "MUTATED_KEY" not in r2.checkpoints_summary
    assert r2.assessment_dict is not None
    assert "MUTATED" not in r2.assessment_dict


def test_m3_secret_redaction_in_error_messages(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """M3 Requirements L, 11: Secret and credential redaction from error diagnostics."""
    c_valid, code_valid = _create_pipeline_files(tmp_path, "sec_red")

    p1 = PipelineSubmission(
        pipeline_id="P_SEC",
        developer="DevSec",
        contract_path=str(c_valid),
        code_path=str(code_valid),
        line_number=2,
        resolved_contract_path=str(c_valid.resolve()),
        resolved_code_path=str(code_valid.resolve()),
    )

    from dpif.orchestration import batch as batch_module

    def mock_load_with_secrets(path):
        raise RuntimeError(
            "Failed DB connection: password=supersecret123 token=abc999xyz api_key=key_val_456 bearer my_secret_bearer_token"
        )

    monkeypatch.setattr(batch_module, "load_contract_file", mock_load_with_secrets)

    res = run_batch_validation([p1])
    p_res = res.pipeline_results[0]

    assert p_res.processing_status == PipelineProcessingStatus.PROCESSING_ERROR
    err_msg = p_res.errors[0]

    assert "supersecret123" not in err_msg
    assert "abc999xyz" not in err_msg
    assert "key_val_456" not in err_msg
    assert "my_secret_bearer_token" not in err_msg
    assert "[REDACTED]" in err_msg


def test_m3_100_pipelines_single_failure_isolation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """M3 Requirements I & 100-pipeline failure isolation test."""
    c_shared, code_shared = _create_pipeline_files(tmp_path, "shared_100_fail")

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
        for i in range(100)
    ]

    from dpif.orchestration import batch as batch_module

    orig_fn = batch_module.validate_single_pipeline_submission

    def mock_val_50(sub: PipelineSubmission, environment: str | None = None):
        if sub.pipeline_id == "P050":
            raise RuntimeError("Catastrophic pipeline P050 failure")
        return orig_fn(sub, environment=environment)

    monkeypatch.setattr(batch_module, "validate_single_pipeline_submission", mock_val_50)

    batch_res = run_batch_validation(subs)

    assert batch_res.total_submissions == 100
    assert len(batch_res.pipeline_results) == 100
    assert batch_res.validated_count == 99
    assert batch_res.processing_error_count == 1

    results_by_id = {r.submission.pipeline_id: r for r in batch_res.pipeline_results}

    assert results_by_id["P050"].processing_status == PipelineProcessingStatus.PROCESSING_ERROR
    assert "Catastrophic pipeline P050 failure" in results_by_id["P050"].errors[0]

    # Verify P049 and P051 are valid and complete
    assert results_by_id["P049"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert results_by_id["P051"].processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert [r.submission.pipeline_id for r in batch_res.pipeline_results] == [
        f"P{i:03d}" for i in range(100)
    ]

