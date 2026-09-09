"""Unit and isolation tests for batch validation orchestrator."""

from pathlib import Path

from dpif.orchestration.batch import run_batch_validation
from dpif.orchestration.models import (
    PipelineProcessingStatus,
    PipelineSubmission,
)


def test_mandatory_pipeline_isolation(tmp_path: Path):
    """MANDATORY ISOLATION TEST:

    Prove that Pipeline A (with critical findings and low score) and Pipeline B
    (with high score and no critical findings) maintain completely independent
    scores, findings, and readiness statuses when run in the exact same batch.
    """
    # Prepare Pipeline A (bad/unoptimized code, low score/not ready)
    contract_a = tmp_path / "contract_a.yaml"
    contract_a.write_text(
        """
contract_id: "P00A"
pipeline_name: "PipelineA_Bad"
environment: "production"
source:
  type: "adls"
  path: "abfss://raw@account.dfs.core.windows.net/data"
  format: "parquet"
  expected_volume_gb: 100.0
target:
  target_id: "t1"
  type: "delta"
  path: "abfss://gold@account.dfs.core.windows.net/out"
sla:
  max_runtime_minutes: 60.0
""",
        encoding="utf-8",
    )
    code_a = tmp_path / "code_a.py"
    # Unoptimized code with bad practices (CROSS JOIN, collect, missing partition filters)
    code_a.write_text(
        """
df1 = spark.read.parquet("path1")
df2 = spark.read.parquet("path2")
res = df1.crossJoin(df2)
res.collect()
res.write.save("out")
""",
        encoding="utf-8",
    )

    # Prepare Pipeline B (good/optimized code, high score/production ready)
    contract_b = tmp_path / "contract_b.yaml"
    contract_b.write_text(
        """
contract_id: "P00B"
pipeline_name: "PipelineB_Good"
environment: "production"
source:
  type: "adls"
  path: "abfss://raw@account.dfs.core.windows.net/data_good"
  format: "parquet"
  expected_volume_gb: 10.0
target:
  target_id: "t2"
  type: "delta"
  path: "abfss://gold@account.dfs.core.windows.net/out_good"
sla:
  max_runtime_minutes: 60.0
""",
        encoding="utf-8",
    )
    code_b = tmp_path / "code_b.py"
    code_b.write_text(
        """
df = spark.read.format("parquet").load("path")
df_filtered = df.filter("date = '2026-01-01'")
df_filtered.write.format("delta").mode("append").save("out")
""",
        encoding="utf-8",
    )

    sub_a = PipelineSubmission(
        pipeline_id="P00A",
        developer="DevA",
        contract_path=str(contract_a),
        code_path=str(code_a),
    )
    sub_b = PipelineSubmission(
        pipeline_id="P00B",
        developer="DevB",
        contract_path=str(contract_b),
        code_path=str(code_b),
    )

    # Run in SAME batch
    batch_res = run_batch_validation([sub_a, sub_b])

    assert len(batch_res.pipeline_results) == 2
    res_a = next(r for r in batch_res.pipeline_results if r.submission.pipeline_id == "P00A")
    res_b = next(r for r in batch_res.pipeline_results if r.submission.pipeline_id == "P00B")

    assert res_a.processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert res_b.processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE

    # Verify independent scores and findings
    diff_readiness = res_a.readiness_status != res_b.readiness_status
    diff_score = res_a.quality_score != res_b.quality_score
    assert diff_readiness or diff_score
    assert res_a.critical_count + res_a.high_count > res_b.critical_count + res_b.high_count
    assert res_b.quality_score is not None and res_a.quality_score is not None
    assert res_b.quality_score > res_a.quality_score


def test_failure_isolation(tmp_path: Path):
    """Verify that a failure/exception in one pipeline does NOT stop the batch."""
    contract_good = tmp_path / "contract_good.yaml"
    contract_good.write_text(
        """
contract_id: "P_GOOD"
pipeline_name: "GoodPipeline"
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
    code_good = tmp_path / "code_good.py"
    code_good.write_text("print('hello')", encoding="utf-8")

    sub_valid1 = PipelineSubmission(
        pipeline_id="P001",
        developer="Dev1",
        contract_path=str(contract_good),
        code_path=str(code_good),
    )
    sub_missing_code = PipelineSubmission(
        pipeline_id="P002",
        developer="Dev2",
        contract_path=str(contract_good),
        code_path=str(tmp_path / "non_existent_code.py"),
    )
    sub_valid2 = PipelineSubmission(
        pipeline_id="P003",
        developer="Dev3",
        contract_path=str(contract_good),
        code_path=str(code_good),
    )

    batch_res = run_batch_validation([sub_valid1, sub_missing_code, sub_valid2])

    assert batch_res.total_submissions == 3
    assert batch_res.validated_count == 2
    assert batch_res.missing_input_count == 1

    p1 = next(r for r in batch_res.pipeline_results if r.submission.pipeline_id == "P001")
    p2 = next(r for r in batch_res.pipeline_results if r.submission.pipeline_id == "P002")
    p3 = next(r for r in batch_res.pipeline_results if r.submission.pipeline_id == "P003")

    assert p1.processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
    assert p2.processing_status == PipelineProcessingStatus.MISSING_INPUT
    assert p3.processing_status == PipelineProcessingStatus.VALIDATION_COMPLETE
