"""End-to-end offline validation: ADLS->Parquet->PySpark->Delta->Job."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.connectors.offline import OfflineDatabricksConnector
from dpif.contract.loader import contract_cluster_job, load_contract_file
from dpif.models import (
    AnalysisMethod,
    CheckpointStatus,
    CollectionMethod,
    DataProfile,
    SchemaColumn,
)


def _load_profile(path: Path) -> DataProfile:
    raw = json.loads(path.read_text(encoding="utf-8"))
    columns = [
        SchemaColumn(
            name=str(c.get("name", "")),
            data_type=str(c.get("data_type", c.get("type", "unknown"))),
            nullable=bool(c.get("nullable", True)),
        )
        for c in (raw.get("schema_columns") or [])
        if isinstance(c, dict) and c.get("name")
    ]
    return DataProfile(
        total_bytes=int(raw["total_bytes"]),
        total_gb=float(raw["total_gb"]),
        file_count=int(raw["file_count"]),
        average_file_size_kb=float(raw["average_file_size_kb"]),
        median_file_size_kb=float(raw.get("median_file_size_kb", 0.0)),
        p95_file_size_kb=float(raw.get("p95_file_size_kb", 0.0)),
        p99_file_size_kb=float(raw.get("p99_file_size_kb", 0.0)),
        min_file_size_kb=float(raw.get("min_file_size_kb", 0.0)),
        max_file_size_kb=float(raw.get("max_file_size_kb", 0.0)),
        record_count=int(raw.get("record_count", 0)),
        partition_count=int(raw.get("partition_count", 0)),
        partition_distribution=dict(raw.get("partition_distribution", {}) or {}),
        column_count=int(raw.get("column_count", 0)),
        analysis_method=AnalysisMethod.METADATA,
        collection_method=CollectionMethod.FIXTURE,
        evidence_source="fixture metadata",
        compression=str(raw.get("compression", "")),
        partition_sizes_gb={
            str(k): float(v) for k, v in (raw.get("partition_sizes_gb", {}) or {}).items()
        },
        partition_record_counts={
            str(k): int(v) for k, v in (raw.get("partition_record_counts", {}) or {}).items()
        },
        schema_columns=columns,
    )


def _run(contract_path: Path, profile_path: Path, code_path: Path):
    contract = load_contract_file(contract_path)
    profile = _load_profile(profile_path)
    contract.source.data_profile = profile
    code_text = code_path.read_text(encoding="utf-8")
    cluster_config, job_config = contract_cluster_job(contract)
    context = {
        "pipeline_name": contract.pipeline_name,
        "pipeline_contract": contract,
        "source": contract.source,
        "data_profile": profile,
        "code_snippet": code_text,
        "code_filename": code_path.name,
        "cluster_config": cluster_config,
        "job_config": job_config,
        "assumptions": {"mode": "offline-fixture"},
        "rule_context": {
            "data_size_gb": profile.total_gb,
            "source_type": contract.source.type.value,
            "workload_type": contract.processing,
        },
    }
    cps = build_all_checkpoints(contract, profile, code_text, cluster_config, job_config)
    return CheckpointEngine().run_all_checkpoints(cps, context)


def test_good_pipeline_passes_evaluable_checks(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "customer_daily_pipeline.yaml",
        metadata_dir / "profile_500gb.json",
        code_dir / "good_pipeline.py",
    )
    assert out["CP-001"].status == CheckpointStatus.PASS
    # dropDuplicates(["order_id"]) at 500 GB is an *appropriate* WARN
    # (shuffle/dedup risk), never a FAIL: static analysis cannot prove cost.
    # CODE-SQL-006 (Expensive DISTINCT) also correctly flags the dropDuplicates.
    assert out["CP-004"].status == CheckpointStatus.WARN
    assert not [f for f in out["CP-004"].findings if f.status == CheckpointStatus.FAIL]
    assert {f.rule_id for f in out["CP-004"].findings} == {
        "CODE-PYSPARK-008",
        "CODE-PYSPARK-015",
        "CODE-SQL-006",
    }
    assert out["CP-007"].status == CheckpointStatus.PASS
    # Honest UNKNOWNs: no pricing / runtime data offline.
    assert out["CP-019"].status == CheckpointStatus.UNKNOWN
    assert out["CP-023"].status == CheckpointStatus.UNKNOWN
    assert out["CP-019"].evidence.recommendation != ""


def test_bad_pipeline_produces_meaningful_failures(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "bad_production_pipeline.yaml",
        metadata_dir / "profile_500gb.json",
        code_dir / "bad_pipeline.py",
    )
    assert out["CP-004"].status == CheckpointStatus.FAIL
    rule_ids = {f.rule_id for f in out["CP-004"].findings}
    assert "CODE-PYSPARK-001" in rule_ids  # blocking collect
    assert out["CP-012"].status == CheckpointStatus.FAIL  # full reload at 2 TB
    assert out["CP-014"].status == CheckpointStatus.FAIL  # no retries
    assert out["CP-020"].status == CheckpointStatus.FAIL  # hardcoded secret
    blocking = [
        f for f in out["CP-004"].findings if f.blocking and f.status == CheckpointStatus.FAIL
    ]
    assert blocking, "expected a blocking FAIL finding"


def test_small_file_profile_warns(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "customer_daily_pipeline.yaml",
        metadata_dir / "profile_small_files.json",
        code_dir / "good_pipeline.py",
    )
    assert out["CP-007"].status == CheckpointStatus.WARN
    assert out["CP-007"].evidence.observed["file_count"] == 1800000


def test_incremental_contract_passes_cp012(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "incremental_pipeline.yaml",
        metadata_dir / "profile_100gb.json",
        code_dir / "good_pipeline.py",
    )
    assert out["CP-012"].status == CheckpointStatus.PASS


def test_bad_pipeline_code_findings_are_structural(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "bad_production_pipeline.yaml",
        metadata_dir / "profile_500gb.json",
        code_dir / "bad_pipeline.py",
    )
    assert out["CP-004"].status == CheckpointStatus.FAIL
    rule_ids = {f.rule_id for f in out["CP-004"].findings}
    assert "CODE-PYSPARK-006" in rule_ids  # AST large collect, blocking
    assert "CODE-PYSPARK-011" in rule_ids  # unjustified cache
    assert "CODE-PYSPARK-012" in rule_ids  # repeated actions
    collect = next(f for f in out["CP-004"].findings if f.rule_id == "CODE-PYSPARK-006")
    assert any("bad_pipeline.py:9" in str(ev) for ev in collect.evidence.evidence)
    # 500 GB fixture profile correctly scopes the data context here.
    assert "500" in str(collect.evidence.evidence)


def test_optimized_small_volume_code_passes(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "small_batch_pipeline.yaml",
        metadata_dir / "profile_10gb.json",
        code_dir / "optimized_pipeline.py",
    )
    assert out["CP-004"].status == CheckpointStatus.PASS
    assert "No high-confidence static anti-patterns detected" in (
        out["CP-004"].evidence.recommendation
    )
    assert out["CP-004"].assumptions["operations_detected"]["JOIN"] == 1


def test_optimized_same_code_large_volume_warns(contracts_dir, metadata_dir, code_dir):
    small = _run(
        contracts_dir / "small_batch_pipeline.yaml",
        metadata_dir / "profile_10gb.json",
        code_dir / "optimized_pipeline.py",
    )
    large = _run(
        contracts_dir / "customer_daily_pipeline.yaml",
        metadata_dir / "profile_500gb.json",
        code_dir / "optimized_pipeline.py",
    )
    assert small["CP-004"].status == CheckpointStatus.PASS
    assert large["CP-004"].status == CheckpointStatus.WARN  # join shuffle at 500 GB


def test_cli_code_section_present(repo_root):
    from dpif.cli import cli

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(repo_root / "examples" / "customer_daily.yaml"),
            "--offline",
        ],
    )
    assert result.exit_code == 0, result.output
    for section in (
        "CODE",
        "Operations detected:",
        "DataFrame flow:",
        "Code findings:",
        "Data context:",
    ):
        assert section in result.output, section


def test_offline_connector_never_fakes_runs(metadata_dir):
    conn = OfflineDatabricksConnector(metadata_dir)
    assert conn.mode() == "offline-fixture"
    assert conn.get_recent_runs(123) is None
    assert conn.get_job(123)["_connector"] == "offline-fixture"


def test_cli_good_contract_exits_zero_and_shows_score(repo_root):
    from dpif.cli import cli

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(repo_root / "examples" / "customer_daily.yaml"),
            "--offline",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Overall Score" in result.output
    assert "NOT_PRODUCTION_READY" in result.output  # honest UNKNOWNs remain


def test_cli_bad_contract_fails_pipeline_but_exits_zero(repo_root):
    from dpif.cli import cli

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(repo_root / "tests" / "fixtures" / "contracts" / "bad_production_pipeline.yaml"),
            "--offline",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "CODE-PYSPARK-001" in result.output
    # Secrets must be masked in reports.
    assert "Sup3rSecret" not in result.output


def test_cli_missing_contract_is_usage_error():
    from dpif.cli import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "--offline"])
    assert result.exit_code == 2


# ------------------------------------------------ Phase 3 scenarios
def test_healthy_parquet_all_evaluable_pass(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "healthy_parquet_pipeline.yaml",
        metadata_dir / "profile_healthy_parquet.json",
        code_dir / "good_pipeline.py",
    )
    for cp_id in ("CP-001", "CP-007", "CP-009", "CP-011", "CP-012"):
        assert out[cp_id].status == CheckpointStatus.PASS, cp_id
    # 250 GB dedup: appropriate WARN only (008/015), no FAIL.
    assert out["CP-004"].status == CheckpointStatus.WARN
    assert not [f for f in out["CP-004"].findings if f.status == CheckpointStatus.FAIL]
    assert out["CP-019"].status == CheckpointStatus.UNKNOWN  # no pricing offline
    assert out["CP-023"].status == CheckpointStatus.UNKNOWN  # no runtime offline


def test_csv_large_volume_warns_source_format(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "csv_large_volume_pipeline.yaml",
        metadata_dir / "profile_csv_large.json",
        code_dir / "good_pipeline.py",
    )
    assert out["CP-001"].status == CheckpointStatus.WARN
    assert any(f.rule_id == "SOURCE-001" for f in out["CP-001"].findings)


def test_schema_drift_fails_data(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "schema_drift_pipeline.yaml",
        metadata_dir / "profile_schema_drift.json",
        code_dir / "good_pipeline.py",
    )
    assert out["CP-007"].status == CheckpointStatus.FAIL
    drift = [f for f in out["CP-007"].findings if f.rule_id == "DATA-003"]
    assert drift and "loyalty_tier" in drift[0].evidence.observed["missing"]


def test_jdbc_large_without_parallel_fails(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "jdbc_large_pipeline.yaml",
        metadata_dir / "profile_jdbc_extract.json",
        code_dir / "good_pipeline.py",
    )
    rule_ids = {f.rule_id for f in out["CP-001"].findings}
    assert "SOURCE-003" in rule_ids
    assert out["CP-001"].status == CheckpointStatus.FAIL


def test_jdbc_incremental_documented_no_source003(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "jdbc_incremental_pipeline.yaml",
        metadata_dir / "profile_jdbc_extract.json",
        code_dir / "good_pipeline.py",
    )
    assert "SOURCE-003" not in {f.rule_id for f in out["CP-001"].findings}


def test_skewed_partitions_warn_data002(contracts_dir, metadata_dir, code_dir):
    out = _run(
        contracts_dir / "customer_daily_pipeline.yaml",
        metadata_dir / "profile_skewed.json",
        code_dir / "good_pipeline.py",
    )
    assert out["CP-007"].status == CheckpointStatus.WARN
    assert any(f.rule_id == "DATA-002" for f in out["CP-007"].findings)


def test_streaming_contract_parses_and_reports_unknowns(contracts_dir, metadata_dir, code_dir):
    contract = load_contract_file(contracts_dir / "streaming_pipeline.yaml")
    assert contract.source.streaming is not None
    assert contract.source.streaming.checkpoint_location.startswith("abfss://")
    out = _run(
        contracts_dir / "streaming_pipeline.yaml",
        metadata_dir / "profile_streaming.json",
        code_dir / "good_pipeline.py",
    )
    # File-level data checks cannot run on a stream: honest UNKNOWN, not PASS.
    assert out["CP-007"].status == CheckpointStatus.UNKNOWN
    assert out["CP-023"].status == CheckpointStatus.UNKNOWN


def test_evidence_coverage_reported_not_scored(contracts_dir, metadata_dir, code_dir):
    from dpif.analyzers.data.coverage import compute_coverage

    out = _run(
        contracts_dir / "healthy_parquet_pipeline.yaml",
        metadata_dir / "profile_healthy_parquet.json",
        code_dir / "good_pipeline.py",
    )
    cov = compute_coverage(out)
    assert cov.total_checks == len(out)
    assert cov.evaluated_checks + cov.unknown_checks == cov.total_checks
    assert 0.0 <= cov.coverage_percentage <= 100.0


def test_cli_data_section_and_coverage(repo_root):
    from dpif.cli import cli

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(repo_root / "examples" / "customer_daily.yaml"),
            "--offline",
        ],
    )
    assert result.exit_code == 0, result.output
    for section in (
        "SOURCE",
        "DATA",
        "Evidence Coverage:",
        "Data Volume:",
        "Peak:",
        "File Count:",
        "Average File Size:",
        "Findings:",
        "fixture metadata",
        "Runtime prediction unavailable",
    ):
        assert section in result.output, section
