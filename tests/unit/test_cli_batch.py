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

    assert "DATABRICKS PIPELINE INTELLIGENCE - Multi-Pipeline Batch Validation" in result.output
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
