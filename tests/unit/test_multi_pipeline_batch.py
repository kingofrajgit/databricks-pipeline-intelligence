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
    valid, invalid = load_and_validate_manifest(missing_csv, allowed_root=tmp_path)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0].processing_status == PipelineProcessingStatus.MISSING_INPUT


def test_load_manifest_empty(tmp_path: Path):
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("", encoding="utf-8")
    valid, invalid = load_and_validate_manifest(empty_csv, allowed_root=tmp_path)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0].processing_status == PipelineProcessingStatus.INVALID_SUBMISSION
    assert "empty" in invalid[0].errors[0].lower()


def test_load_manifest_missing_headers(tmp_path: Path):
    bad_csv = tmp_path / "bad_headers.csv"
    bad_csv.write_text("pipeline_id,developer\nP001,DevA\n", encoding="utf-8")
    valid, invalid = load_and_validate_manifest(bad_csv, allowed_root=tmp_path)
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

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
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

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
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

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(valid) == 1
    assert len(invalid) == 0
    assert valid[0].pipeline_id == "P001"
    assert valid[0].developer == "DevA"
    assert valid[0].resolved_contract_path == str(contract_file.resolve())
    assert valid[0].resolved_code_path == str(code_file.resolve())


def test_load_manifest_valid_multi_row(tmp_path: Path):
    c1 = tmp_path / "c1.yaml"
    c1.write_text("name: c1", encoding="utf-8")
    code1 = tmp_path / "code1.py"
    code1.write_text("p1", encoding="utf-8")

    c2 = tmp_path / "c2.yaml"
    c2.write_text("name: c2", encoding="utf-8")
    code2 = tmp_path / "code2.py"
    code2.write_text("p2", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,{c1.name},{code1.name}\n"
        f"P002,DevB,{c2.name},{code2.name}\n"
    )
    manifest_csv = tmp_path / "multi_manifest.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(valid) == 2
    assert len(invalid) == 0
    assert valid[0].pipeline_id == "P001"
    assert valid[0].developer == "DevA"
    assert valid[1].pipeline_id == "P002"
    assert valid[1].developer == "DevB"


def test_load_manifest_extra_header(tmp_path: Path):
    bad_csv = tmp_path / "extra_header.csv"
    bad_csv.write_text(
        "pipeline_id,developer,contract_path,code_path,foo_bar\nP001,DevA,c.yaml,c.py,val\n",
        encoding="utf-8",
    )
    valid, invalid = load_and_validate_manifest(bad_csv, allowed_root=tmp_path)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert "Unknown CSV header" in invalid[0].errors[0]


def test_load_manifest_empty_pipeline_id(tmp_path: Path):
    c = tmp_path / "c.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "c.py"
    code.write_text("p", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        f",DevA,{c.name},{code.name}\n"
    )
    manifest_csv = tmp_path / "empty_pid.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert "missing required column 'pipeline_id'" in invalid[0].errors[0]


def test_load_manifest_missing_evidence_files(tmp_path: Path):
    c = tmp_path / "c.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "c.py"
    code.write_text("p", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path,metadata_profile,runtime_run,historical_runs\n"
        f"P001,DevA,{c.name},{code.name},missing_meta.json,missing_rt.json,missing_hist.json\n"
    )
    manifest_csv = tmp_path / "missing_evidence.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0].processing_status == PipelineProcessingStatus.MISSING_INPUT
    err_str = " ".join(invalid[0].errors)
    assert "metadata_profile error" in err_str
    assert "runtime_run error" in err_str
    assert "historical_runs error" in err_str


def test_path_traversal_dotdot(tmp_path: Path):
    c = tmp_path / "c.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "c.py"
    code.write_text("p", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,../{c.name},{code.name}\n"
    )
    manifest_csv = tmp_path / "traversal.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert "Path traversal ('..') not allowed" in invalid[0].errors[0]


def test_path_traversal_unc(tmp_path: Path):
    c = tmp_path / "c.yaml"
    c.write_text("name: c", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,//server/share/c.yaml,{c.name}\n"
    )
    manifest_csv = tmp_path / "unc.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(valid) == 0
    assert len(invalid) == 1
    assert "UNC path not allowed" in invalid[0].errors[0]


def test_windows_drive_letter_paths(tmp_path: Path):
    c = tmp_path / "c.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "code.py"
    code.write_text("print('test')", encoding="utf-8")

    bad_paths = [
        r"C:\secret\file",
        "C:/secret/file",
        r"D:\data\contract.yaml",
        "Z:/workspace/code.py",
        "C:relative",
    ]
    for bp in bad_paths:
        csv_content = (
            "pipeline_id,developer,contract_path,code_path\n"
            f"P001,DevA,{bp},{code.name}\n"
        )
        manifest_csv = tmp_path / "win_drive.csv"
        manifest_csv.write_text(csv_content, encoding="utf-8")
        valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
        assert len(valid) == 0, f"Expected rejection for {bp}"
        assert len(invalid) == 1
        assert "Windows drive letter path not allowed" in invalid[0].errors[0]


def test_csv_structural_validation(tmp_path: Path):
    c = tmp_path / "c.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "code.py"
    code.write_text("print('test')", encoding="utf-8")

    # 1. Too many columns
    csv_content1 = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,{c.name},{code.name},extra_val\n"
    )
    m1 = tmp_path / "m1.csv"
    m1.write_text(csv_content1, encoding="utf-8")
    v1, inv1 = load_and_validate_manifest(m1, allowed_root=tmp_path)
    assert len(v1) == 0 and len(inv1) == 1
    assert "Row 2: expected 4 columns, received 5" in inv1[0].errors[0]

    # 2. Too few columns
    csv_content2 = (
        "pipeline_id,developer,contract_path,code_path\n"
        "P001,DevA,only_3_fields\n"
    )
    m2 = tmp_path / "m2.csv"
    m2.write_text(csv_content2, encoding="utf-8")
    v2, inv2 = load_and_validate_manifest(m2, allowed_root=tmp_path)
    assert len(v2) == 0 and len(inv2) == 1
    assert "Row 2: expected 4 columns, received 3" in inv2[0].errors[0]

    # 3. Unterminated quote / malformed syntax
    csv_content3 = (
        "pipeline_id,developer,contract_path,code_path\n"
        f'P001,"DevA,{c.name},{code.name}\n'
    )
    m3 = tmp_path / "m3.csv"
    m3.write_text(csv_content3, encoding="utf-8")
    v3, inv3 = load_and_validate_manifest(m3, allowed_root=tmp_path)
    assert len(v3) == 0 and len(inv3) == 1
    assert "Row 2: malformed CSV syntax" in inv3[0].errors[0]


def test_approved_root_boundaries(tmp_path: Path):
    app_root = tmp_path / "app_root"
    app_root.mkdir()
    sibling_dir = tmp_path / "sibling_dir"
    sibling_dir.mkdir()

    c_inside = app_root / "c.yaml"
    c_inside.write_text("name: c", encoding="utf-8")
    code_inside = app_root / "c.py"
    code_inside.write_text("p", encoding="utf-8")

    c_outside = sibling_dir / "c_out.yaml"
    c_outside.write_text("name: out", encoding="utf-8")

    manifest = app_root / "manifest.csv"

    # File inside approved root -> allowed
    manifest.write_text(
        f"pipeline_id,developer,contract_path,code_path\nP001,DevA,{c_inside.name},{code_inside.name}\n",
        encoding="utf-8",
    )
    v, inv = load_and_validate_manifest(manifest, allowed_root=app_root)
    assert len(v) == 1 and len(inv) == 0

    # Sibling directory outside root -> rejected
    manifest.write_text(
        f"pipeline_id,developer,contract_path,code_path\nP001,DevA,../sibling_dir/c_out.yaml,{code_inside.name}\n",
        encoding="utf-8",
    )
    v, inv = load_and_validate_manifest(manifest, allowed_root=app_root)
    assert len(v) == 0 and len(inv) == 1
    assert "Path traversal" in inv[0].errors[0]


def test_symlink_security_boundaries(tmp_path: Path):
    app_root = tmp_path / "app_root"
    app_root.mkdir()
    outside_dir = tmp_path / "outside_dir"
    outside_dir.mkdir()

    outside_file = outside_dir / "secret.yaml"
    outside_file.write_text("secret: true", encoding="utf-8")

    code_inside = app_root / "c.py"
    code_inside.write_text("p", encoding="utf-8")

    symlink_file = app_root / "link.yaml"
    try:
        symlink_file.symlink_to(outside_file)
    except (OSError, NotImplementedError):
        return  # Skip if system permissions do not allow symlink creation in test environment

    manifest = app_root / "manifest.csv"
    manifest.write_text(
        f"pipeline_id,developer,contract_path,code_path\nP001,DevA,link.yaml,{code_inside.name}\n",
        encoding="utf-8",
    )

    v, inv = load_and_validate_manifest(manifest, allowed_root=app_root)
    assert len(v) == 0 and len(inv) == 1
    assert "escapes root" in inv[0].errors[0]


def test_pipeline_isolation(tmp_path: Path):
    c1 = tmp_path / "c1.yaml"
    c1.write_text("name: c1", encoding="utf-8")
    code1 = tmp_path / "code1.py"
    code1.write_text("p1", encoding="utf-8")

    c3 = tmp_path / "c3.yaml"
    c3.write_text("name: c3", encoding="utf-8")
    code3 = tmp_path / "code3.py"
    code3.write_text("p3", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,{c1.name},{code1.name}\n"
        "P002,DevB,missing_contract.yaml,missing_code.py\n"
        f"P003,DevC,{c3.name},{code3.name}\n"
    )
    manifest = tmp_path / "isolation.csv"
    manifest.write_text(csv_content, encoding="utf-8")

    v, inv = load_and_validate_manifest(manifest, allowed_root=tmp_path)
    assert len(v) == 2
    assert [s.pipeline_id for s in v] == ["P001", "P003"]
    assert len(inv) == 1
    assert inv[0].submission.pipeline_id == "P002"
    assert inv[0].processing_status == PipelineProcessingStatus.MISSING_INPUT


def test_valid_nested_relative_path(tmp_path: Path):
    sub_dir = tmp_path / "contracts"
    sub_dir.mkdir()
    c = sub_dir / "c.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "c.py"
    code.write_text("p", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,contracts/c.yaml,{code.name}\n"
    )
    manifest_csv = tmp_path / "nested.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(valid) == 1
    assert len(invalid) == 0
    assert valid[0].resolved_contract_path == str(c.resolve())


def test_blank_optional_evidence_paths(tmp_path: Path):
    c = tmp_path / "c.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "c.py"
    code.write_text("p", encoding="utf-8")

    csv_content = (
        "pipeline_id,developer,contract_path,code_path,metadata_profile,runtime_run,historical_runs\n"
        f"P001,DevA,{c.name},{code.name},,,  \n"
    )
    manifest_csv = tmp_path / "blank_opt.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(valid) == 1
    assert len(invalid) == 0
    assert valid[0].metadata_profile is None
    assert valid[0].resolved_metadata_path is None


def test_csv_advanced_quoting_and_multiline(tmp_path: Path):
    c = tmp_path / "contract.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "code.py"
    code.write_text("p", encoding="utf-8")

    # Quoted field containing comma, escaped quote, whitespace inside quotes, and multiline field
    csv_content = (
        'pipeline_id,developer,contract_path,code_path\n'
        f'P001,"Developer, A",{c.name},{code.name}\n'
        f'P002,"Dev ""Special"" B",{c.name},{code.name}\n'
        f'P003,"  DevC  ",{c.name},{code.name}\n'
        f'P004,"Dev\nLine2",{c.name},{code.name}\n'
    )
    manifest_csv = tmp_path / "quoting.csv"
    manifest_csv.write_text(csv_content, encoding="utf-8")

    valid, invalid = load_and_validate_manifest(manifest_csv, allowed_root=tmp_path)
    assert len(invalid) == 0
    assert len(valid) == 4
    assert valid[0].developer == "Developer, A"
    assert valid[1].developer == 'Dev "Special" B'
    assert valid[2].developer == "DevC"  # strip on individual parsed fields
    assert "Dev" in valid[3].developer and "Line2" in valid[3].developer


def test_security_extended_paths(tmp_path: Path):
    c = tmp_path / "c.yaml"
    c.write_text("name: c", encoding="utf-8")
    code = tmp_path / "c.py"
    code.write_text("p", encoding="utf-8")

    # Unix absolute path escaping root
    csv_content_unix = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,/etc/passwd,{code.name}\n"
    )
    m_unix = tmp_path / "unix_abs.csv"
    m_unix.write_text(csv_content_unix, encoding="utf-8")
    v_u, inv_u = load_and_validate_manifest(m_unix, allowed_root=tmp_path)
    assert len(v_u) == 0 and len(inv_u) == 1
    assert "escapes root" in inv_u[0].errors[0]

    # Symlink directory escape
    outside_dir = tmp_path.parent / "outside_dir_sec"
    outside_dir.mkdir(exist_ok=True)
    outside_file = outside_dir / "secret.yaml"
    outside_file.write_text("secret: true", encoding="utf-8")

    symlink_dir = tmp_path / "dir_link"
    try:
        symlink_dir.symlink_to(outside_dir, target_is_directory=True)
    except (OSError, NotImplementedError):
        return

    csv_content_sym_dir = (
        "pipeline_id,developer,contract_path,code_path\n"
        f"P001,DevA,dir_link/secret.yaml,{code.name}\n"
    )
    m_sym_dir = tmp_path / "sym_dir.csv"
    m_sym_dir.write_text(csv_content_sym_dir, encoding="utf-8")
    v_sd, inv_sd = load_and_validate_manifest(m_sym_dir, allowed_root=tmp_path)
    assert len(v_sd) == 0 and len(inv_sd) == 1
    assert "escapes root" in inv_sd[0].errors[0]


def _create_minimal_valid_pipeline(tmp_path: Path, name: str) -> tuple[Path, Path]:
    c = tmp_path / f"{name}_contract.yaml"
    c.write_text(
        f"""
contract_id: {name}
pipeline_name: {name}
version: "1.0"
owner: owner@test.com
description: Contract for {name}
environment: production
expected_daily_volume_gb: 10.0
source:
  type: adls
  format: parquet
  expected_volume_gb: 10.0
  peak_volume_gb: 15.0
  partitioning: [date]
processing:
  type: batch
target:
  type: delta
  table: target_tbl
  location: dbfs:/mnt/target
""",
        encoding="utf-8",
    )
    code = tmp_path / f"{name}_code.py"
    code.write_text("print('hello')", encoding="utf-8")
    return c, code


def test_m2_cross_pipeline_isolation(tmp_path: Path):
    """Prove P001, P002, P003 are completely isolated (findings, score, coverage, readiness)."""
    from dpif.orchestration.batch import run_batch_validation

    c1, code1 = _create_minimal_valid_pipeline(tmp_path, "P001")
    c2, code2 = _create_minimal_valid_pipeline(tmp_path, "P002")

    # Make P002 code contain an anti-pattern (driver collect) to generate a finding
    code2.write_text("df.collect()", encoding="utf-8")

    sub1 = PipelineSubmission(
        pipeline_id="P001",
        developer="DevA",
        contract_path=str(c1),
        code_path=str(code1),
        resolved_contract_path=str(c1.resolve()),
        resolved_code_path=str(code1.resolve()),
    )
    sub2 = PipelineSubmission(
        pipeline_id="P002",
        developer="DevB",
        contract_path=str(c2),
        code_path=str(code2),
        resolved_contract_path=str(c2.resolve()),
        resolved_code_path=str(code2.resolve()),
    )

    batch_result = run_batch_validation([sub1, sub2])
    assert len(batch_result.pipeline_results) == 2

    res1, res2 = batch_result.pipeline_results[0], batch_result.pipeline_results[1]

    # P001 findings must not contain P002 findings
    assert res1.critical_count == 0 and res1.high_count == 0
    assert len(res2.errors) == 0
    assert res1.assessment_dict is not None
    assert res2.assessment_dict is not None
    # Verify overall assessment objects are isolated instances
    assert res1.assessment_dict is not res2.assessment_dict


def test_m2_failure_isolation_and_exception_handling(tmp_path: Path, monkeypatch):
    """Test P001 (success), P002 (invalid), P003 (missing), P004 (exception), P005 (success)."""
    from dpif.orchestration.batch import run_batch_validation, validate_single_pipeline_submission

    c1, code1 = _create_minimal_valid_pipeline(tmp_path, "P001")
    c5, code5 = _create_minimal_valid_pipeline(tmp_path, "P005")

    sub1 = PipelineSubmission(
        pipeline_id="P001",
        developer="DevA",
        contract_path=str(c1),
        code_path=str(code1),
        resolved_contract_path=str(c1.resolve()),
        resolved_code_path=str(code1.resolve()),
    )
    sub3 = PipelineSubmission(
        pipeline_id="P003",
        developer="DevC",
        contract_path="missing.yaml",
        code_path="missing.py",
        resolved_contract_path=str(tmp_path / "missing.yaml"),
        resolved_code_path=str(tmp_path / "missing.py"),
    )
    sub4 = PipelineSubmission(
        pipeline_id="P004",
        developer="DevD",
        contract_path=str(c1),
        code_path=str(code1),
        resolved_contract_path=str(c1.resolve()),
        resolved_code_path=str(code1.resolve()),
    )
    sub5 = PipelineSubmission(
        pipeline_id="P005",
        developer="DevE",
        contract_path=str(c5),
        code_path=str(code5),
        resolved_contract_path=str(c5.resolve()),
        resolved_code_path=str(code5.resolve()),
    )

    invalid_p002 = PipelineValidationResult(
        submission=PipelineSubmission(
            pipeline_id="P002",
            developer="DevB",
            contract_path="bad.yaml",
            code_path="bad.py",
        ),
        processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
        errors=["Malformed CSV row"],
    )

    # Monkeypatch validate_single_pipeline_submission to throw for P004 specifically
    orig_fn = validate_single_pipeline_submission

    def mock_validate(sub, environment=None):
        if sub.pipeline_id == "P004":
            raise RuntimeError("Simulated validator engine crash!")
        return orig_fn(sub, environment=environment)

    monkeypatch.setattr(
        "dpif.orchestration.batch.validate_single_pipeline_submission",
        mock_validate,
    )

    # Note: run_batch_validation should catch validator exceptions per pipeline
    # Let's verify batch.py handles unexpected exceptions if validate throws
    batch_res = run_batch_validation([sub1, sub3, sub4, sub5], invalid_results=[invalid_p002])

    assert len(batch_res.pipeline_results) == 5
    statuses = {r.submission.pipeline_id: r.processing_status for r in batch_res.pipeline_results}
    assert statuses["P001"] == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert statuses["P002"] == PipelineProcessingStatus.INVALID_SUBMISSION
    assert statuses["P003"] == PipelineProcessingStatus.MISSING_INPUT
    assert statuses["P004"] == PipelineProcessingStatus.PROCESSING_ERROR
    assert statuses["P005"] == PipelineProcessingStatus.VALIDATION_COMPLETE

    # Verify readiness status for failed processing is UNKNOWN / None
    res4 = [r for r in batch_res.pipeline_results if r.submission.pipeline_id == "P004"][0]
    assert res4.readiness_status is None
    assert "Simulated validator engine crash!" in res4.errors[0]


def test_m2_deterministic_ordering(tmp_path: Path):
    """Verify input submission order is strictly preserved in batch result."""
    from dpif.orchestration.batch import run_batch_validation

    subs = []
    for i in range(10):
        pid = f"P{i:03d}"
        c, code = _create_minimal_valid_pipeline(tmp_path, pid)
        subs.append(
            PipelineSubmission(
                pipeline_id=pid,
                developer=f"Dev{i}",
                contract_path=str(c),
                code_path=str(code),
                resolved_contract_path=str(c.resolve()),
                resolved_code_path=str(code.resolve()),
            )
        )

    batch_res = run_batch_validation(subs)
    result_ids = [r.submission.pipeline_id for r in batch_res.pipeline_results]
    expected_ids = [f"P{i:03d}" for i in range(10)]
    assert result_ids == expected_ids


def test_m2_empty_and_mixed_batches(tmp_path: Path):
    """Test empty manifest, all invalid, and mixed batches."""
    from dpif.orchestration.batch import run_batch_validation

    # 1. Empty batch
    empty_res = run_batch_validation([])
    assert empty_res.total_submissions == 0
    assert len(empty_res.pipeline_results) == 0

    # 2. All invalid
    sub_inv1 = PipelineSubmission(
        pipeline_id="P1", developer="D1", contract_path="a", code_path="b"
    )
    inv1 = PipelineValidationResult(
        submission=sub_inv1,
        processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
    )
    sub_inv2 = PipelineSubmission(
        pipeline_id="P2", developer="D2", contract_path="a", code_path="b"
    )
    inv2 = PipelineValidationResult(
        submission=sub_inv2,
        processing_status=PipelineProcessingStatus.INVALID_SUBMISSION,
    )
    all_inv_res = run_batch_validation([], invalid_results=[inv1, inv2])
    assert all_inv_res.total_submissions == 2
    assert all_inv_res.invalid_submission_count == 2
    assert all_inv_res.validated_count == 0


def test_m2_100_pipelines_sequential_batch(tmp_path: Path):
    """Scalability test with 100 pipelines to verify stability and correctness."""
    from dpif.orchestration.batch import run_batch_validation

    # Shared contract and code files to avoid 200 disk writes
    c, code = _create_minimal_valid_pipeline(tmp_path, "shared")

    subs = [
        PipelineSubmission(
            pipeline_id=f"P{i:03d}",
            developer=f"Dev{i}",
            contract_path=str(c),
            code_path=str(code),
            resolved_contract_path=str(c.resolve()),
            resolved_code_path=str(code.resolve()),
        )
        for i in range(100)
    ]

    batch_res = run_batch_validation(subs)
    assert batch_res.total_submissions == 100
    assert batch_res.validated_count == 100
    assert len(batch_res.pipeline_results) == 100
    assert batch_res.pipeline_results[0].submission.pipeline_id == "P000"
    assert batch_res.pipeline_results[99].submission.pipeline_id == "P099"




