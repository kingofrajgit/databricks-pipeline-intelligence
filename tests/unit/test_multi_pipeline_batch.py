from pathlib import Path

from dpif.orchestration.manifest import load_and_validate_manifest
from dpif.orchestration.models import (
    BatchValidationResult,
    PipelineProcessingStatus,
    PipelineSubmission,
    PipelineValidationResult,
)


def test_pipeline_submission_to_dict():
    sub = PipelineSubmission(
        pipeline_id="P001",
        developer="DeveloperA",
        contract_path="contracts/c1.yaml",
        code_path="code/c1.py",
        metadata_profile="meta/c1.json",
    )
    d = sub.to_dict()
    assert d["pipeline_id"] == "P001"
    assert d["developer"] == "DeveloperA"
    assert d["contract_path"] == "contracts/c1.yaml"
    assert d["metadata_profile"] == "meta/c1.json"


def test_batch_validation_result_recompute():
    sub1 = PipelineSubmission(
        pipeline_id="P001",
        developer="DevA",
        contract_path="c.yaml",
        code_path="c.py",
    )
    sub2 = PipelineSubmission(
        pipeline_id="P002",
        developer="DevB",
        contract_path="c2.yaml",
        code_path="c2.py",
    )

    r1 = PipelineValidationResult(
        submission=sub1,
        processing_status=PipelineProcessingStatus.VALIDATION_COMPLETE,
        readiness_status="PRODUCTION_READY",
        critical_count=0,
        high_count=1,
    )
    r2 = PipelineValidationResult(
        submission=sub2,
        processing_status=PipelineProcessingStatus.MISSING_INPUT,
        errors=["Missing contract file"],
    )

    batch = BatchValidationResult(pipeline_results=[r1, r2])
    batch.recompute_summaries()

    assert batch.total_submissions == 2
    assert batch.validated_count == 1
    assert batch.missing_input_count == 1
    assert batch.production_ready_count == 1
    assert batch.total_high_findings == 1


def test_load_manifest_non_existent(tmp_path: Path):
    missing_csv = tmp_path / "non_existent.csv"
    valid, invalid = load_and_validate_manifest(missing_csv)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0].processing_status == PipelineProcessingStatus.MISSING_INPUT


def test_load_manifest_empty(tmp_path: Path):
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("", encoding="utf-8")
    valid, invalid = load_and_validate_manifest(empty_csv)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0].processing_status == PipelineProcessingStatus.INVALID_SUBMISSION
    assert "empty" in invalid[0].errors[0].lower()


def test_load_manifest_missing_headers(tmp_path: Path):
    bad_csv = tmp_path / "bad_headers.csv"
    bad_csv.write_text("pipeline_id,developer\nP001,DevA\n", encoding="utf-8")
    valid, invalid = load_and_validate_manifest(bad_csv)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert "Missing required CSV header" in invalid[0].errors[0]


def test_load_manifest_duplicate_pipeline_id(tmp_path: Path):
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text("pipeline_name: test", encoding="utf-8")
    code_file = tmp_path / "code.py"
    code_file.write_text("print('test')", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,{contract_file.name},{code_file.name}\n"
        f"P001,DevB,{contract_file.name},{code_file.name}\n"
    )
    manifest_csv = tmp_path / "dup_manifest.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv)
    assert len(valid) == 1
    assert len(invalid) == 1
    assert "duplicate pipeline_id" in invalid[0].errors[0]


def test_load_manifest_missing_files(tmp_path: Path):
    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        "P001,DevA,missing_contract.yaml,missing_code.py\n"
    )
    manifest_csv = tmp_path / "missing_files.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0].processing_status == PipelineProcessingStatus.MISSING_INPUT
    assert len(invalid[0].errors) >= 2


def test_load_manifest_valid(tmp_path: Path):
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text("pipeline_name: test", encoding="utf-8")
    code_file = tmp_path / "code.py"
    code_file.write_text("print('test')", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,{contract_file.name},{code_file.name}\n"
    )
    manifest_csv = tmp_path / "valid_manifest.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv)
    assert len(valid) == 1
    assert len(invalid) == 0
    assert valid[0].pipeline_id == "P001"
    assert valid[0].developer == "DevA"
    assert valid[0].resolved_contract_path == str(contract_file.resolve())
    assert valid[0].resolved_code_path == str(code_file.resolve())
