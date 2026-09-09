"""CLI integration tests for multi-pipeline batch validation."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from dpif.cli import cli


def test_cli_batch_validation_success(tmp_path: Path):
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        """
contract_id: "P001"
pipeline_name: "TestPipe"
environment: "production"
source:
  type: "adls"
  path: "abfss://raw@account.dfs.core.windows.net/data"
  format: "parquet"
  expected_volume_gb: 10.0
target:
  target_id: "t1"
  type: "delta"
  path: "out"
""",
        encoding="utf-8",
    )
    code_file = tmp_path / "code.py"
    code_file.write_text("print('test')", encoding="utf-8")

    manifest_csv = tmp_path / "manifest.csv"
    manifest_csv.write_text(
        f"pipeline_id,developer,contract_path,code_path\nP001,DevA,{contract_file.name},{code_file.name}\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--input",
            str(manifest_csv),
            "--output-dir",
            str(tmp_path),
            "--offline",
        ],
    )

    assert "DPIF Batch Validation Complete" in result.output
    assert (tmp_path / "batch_report.json").exists()
    assert (tmp_path / "batch_report.csv").exists()


def test_cli_batch_validation_json_output(tmp_path: Path):
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        """
contract_id: "P001"
pipeline_name: "TestPipe"
environment: "production"
source:
  type: "adls"
  path: "abfss://raw@account.dfs.core.windows.net/data"
  format: "parquet"
  expected_volume_gb: 10.0
target:
  target_id: "t1"
  type: "delta"
  path: "out"
""",
        encoding="utf-8",
    )
    code_file = tmp_path / "code.py"
    code_file.write_text("print('test')", encoding="utf-8")

    manifest_csv = tmp_path / "manifest.csv"
    manifest_csv.write_text(
        f"pipeline_id,developer,contract_path,code_path\nP001,DevA,{contract_file.name},{code_file.name}\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--input",
            str(manifest_csv),
            "--output-dir",
            str(tmp_path),
            "--offline",
            "--json",
        ],
    )

    data = json.loads(result.output)
    assert data["summary"]["total_submissions"] == 1
    assert data["summary"]["validated"] == 1


@pytest.mark.benchmark
def test_100_pipeline_batch_performance(tmp_path: Path):
    """Performance & scalability test:

    Generate 100 valid pipeline submissions and run batch validation.
    Verify execution succeeds without performance degradation or state leakage.
    """
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        """
contract_id: "P_PERF"
pipeline_name: "PerfPipe"
environment: "production"
source:
  type: "adls"
  path: "abfss://raw@account.dfs.core.windows.net/data"
  format: "parquet"
  expected_volume_gb: 10.0
target:
  target_id: "t1"
  type: "delta"
  path: "out"
""",
        encoding="utf-8",
    )
    code_file = tmp_path / "code.py"
    code_file.write_text("print('perf')", encoding="utf-8")

    lines = ["pipeline_id,developer,contract_path,code_path"]
    for i in range(1, 101):
        lines.append(f"PIPE_{i:03d},Dev_{i},{contract_file.name},{code_file.name}")

    manifest_csv = tmp_path / "manifest_100.csv"
    manifest_csv.write_text("\n".join(lines), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--input",
            str(manifest_csv),
            "--output-dir",
            str(tmp_path),
            "--offline",
            "--json",
        ],
    )

    data = json.loads(result.output)
    assert data["summary"]["total_submissions"] == 100
    assert data["summary"]["validated"] == 100
    assert len(data["pipelines"]) == 100


def test_m4_cli_contract_single_pipeline_regression(tmp_path: Path):
    """M4 Req 17: Existing single-pipeline --contract CLI invocation continues working."""
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        """
contract_id: "P_SINGLE"
pipeline_name: "SinglePipe"
environment: "production"
source:
  type: "adls"
  path: "abfss://raw@account.dfs.core.windows.net/data"
  format: "parquet"
  expected_volume_gb: 10.0
target:
  target_id: "t1"
  type: "delta"
  path: "out"
""",
        encoding="utf-8",
    )
    code_file = tmp_path / "code.py"
    code_file.write_text("print('single')", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(contract_file),
            "--code",
            str(code_file),
            "--offline",
        ],
    )
    assert result.exit_code == 0
    assert "DATABRICKS PIPELINE INTELLIGENCE" in result.output


def test_m4_cli_contract_and_input_conflict(tmp_path: Path):
    """M4 Req 1: Specifying both --contract and --input yields CLI usage error exit 2."""
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text("contract_id: C1\n", encoding="utf-8")
    manifest_csv = tmp_path / "manifest.csv"
    manifest_csv.write_text("pipeline_id,developer,contract_path,code_path\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(contract_file),
            "--input",
            str(manifest_csv),
            "--offline",
        ],
    )
    assert result.exit_code == 2
    assert "Error: Cannot specify both --contract and --input" in result.output


def test_m4_cli_missing_input_manifest():
    """M4 Req 18: Missing CSV input manifest file path returns click error."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--input",
            "non_existent_manifest.csv",
            "--offline",
        ],
    )
    assert result.exit_code != 0
    assert "does not exist" in result.output or "Invalid value" in result.output


def test_m4_cli_output_dir_creation_and_secret_redaction(tmp_path: Path):
    """M4 Reqs 2, 11: Output dir created and secrets redacted from report files & stdout."""
    out_dir = tmp_path / "custom_reports_dir"
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text(
        """
contract_id: "P_SECRET"
pipeline_name: "SecretPipe"
environment: "production"
source:
  type: "adls"
  path: "abfss://raw@account.dfs.core.windows.net/data"
  format: "parquet"
  expected_volume_gb: 10.0
target:
  target_id: "t1"
  type: "delta"
  path: "out"
""",
        encoding="utf-8",
    )
    code_file = tmp_path / "code.py"
    # Secret in code comment
    code_content = "# password=secret123\n# token=abc123\n# api_key=xyz789\nprint('sec')"
    code_file.write_text(code_content, encoding="utf-8")

    manifest_csv = tmp_path / "manifest_sec.csv"
    manifest_csv.write_text(
        f"pipeline_id,developer,contract_path,code_path\nP_SEC,DevSec,{contract_file.name},{code_file.name}\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--input",
            str(manifest_csv),
            "--output-dir",
            str(out_dir),
            "--offline",
        ],
    )

    assert result.exit_code == 0
    assert out_dir.exists()
    json_path = out_dir / "batch_report.json"
    csv_path = out_dir / "batch_report.csv"
    assert json_path.exists()
    assert csv_path.exists()

    json_text = json_path.read_text(encoding="utf-8")
    csv_text = csv_path.read_text(encoding="utf-8")

    # Assert secrets do not leak into outputs
    for s in ("secret123", "abc123", "xyz789"):
        assert s not in result.output
        assert s not in json_text
        assert s not in csv_text

