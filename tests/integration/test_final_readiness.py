"""Integration tests for CP-FINAL Production Readiness & Pipeline Decision.

Validates end-to-end scenarios:
- Scenario A: Healthy pipeline release decision.
- Scenario B: Critical driver collection blocking failure.
- Scenario C: Missing runtime evidence under strict policy.
- Scenario D: Advisory warnings producing PRODUCTION_READY_WITH_WARNINGS.
- Scenario E: Cluster configuration drift detection.
- Scenario F: SLA projected violation decoupled from actual SLA status.
- Security redaction audit.
- CLI JSON machine-readable output format.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.cli import cli
from dpif.contract.loader import contract_cluster_job, load_contract_file
from dpif.models import (
    AnalysisMethod,
    CollectionMethod,
    DataProfile,
    SchemaColumn,
)
from dpif.readiness.models import (
    ProductionReadinessStatus,
    ReadinessPolicy,
)
from dpif.runtime.normalization import normalize_runtime_payload


def _load_run_fixture(path: Path) -> Any:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return normalize_runtime_payload(raw)


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


class TestFinalReadinessIntegration:
    def test_scenario_a_healthy_pipeline_readiness(
        self, contracts_dir: Path, metadata_dir: Path, code_dir: Path, fixtures_dir: Path
    ) -> None:
        contract = load_contract_file(str(contracts_dir / "healthy_parquet_pipeline.yaml"))
        contract.expected_daily_volume_gb = 10.0
        contract.peak_daily_volume_gb = 15.0
        contract.source.expected_volume_gb = 10.0
        contract.source.peak_volume_gb = 15.0
        profile = _load_profile(metadata_dir / "profile_10gb.json")
        contract.source.data_profile = profile
        code_text = (code_dir / "optimized_pipeline.py").read_text(encoding="utf-8")
        runtime_path = fixtures_dir / "runtime" / "healthy_run.json"
        runtime_data = _load_run_fixture(runtime_path) if runtime_path.exists() else None

        cluster_config, job_config = contract_cluster_job(contract)
        cps = build_all_checkpoints(
            contract=contract,
            data_profile=profile,
            code_text=code_text,
            cluster_config=cluster_config,
            job_config=job_config,
            runtime_data=runtime_data,
        )
        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": contract.source,
            "data_profile": profile,
            "code_snippet": code_text,
            "cluster_config": cluster_config,
            "job_config": job_config,
            "runtime_data": runtime_data,
            "runtime_run": runtime_data,
            "rule_context": {
                "data_size_gb": profile.total_gb,
                "source_type": contract.source.type.value,
                "workload_type": contract.processing,
            },
        }
        engine = CheckpointEngine()
        engine.run_all_checkpoints(cps, context)

        assessment = context.get("production_readiness_assessment")
        assert assessment is not None
        assert assessment.status in (
            ProductionReadinessStatus.PRODUCTION_READY,
            ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS,
        )
        assert len(assessment.critical_findings) == 0
        assert len(assessment.blocking_findings) == 0

    def test_scenario_b_critical_driver_collection_blocking_failure(
        self, contracts_dir: Path, code_dir: Path
    ) -> None:
        contract = load_contract_file(str(contracts_dir / "bad_production_pipeline.yaml"))
        code_text = (code_dir / "bad_pipeline.py").read_text(encoding="utf-8")
        cluster_config, job_config = contract_cluster_job(contract)

        cps = build_all_checkpoints(
            contract=contract,
            data_profile=None,
            code_text=code_text,
            cluster_config=cluster_config,
            job_config=job_config,
        )
        context = {
            "pipeline_contract": contract,
            "code_snippet": code_text,
            "cluster_config": cluster_config,
            "job_config": job_config,
        }
        engine = CheckpointEngine()
        results = engine.run_all_checkpoints(cps, context)

        assessment = context.get("production_readiness_assessment")
        assert assessment is not None
        assert assessment.status == ProductionReadinessStatus.NOT_PRODUCTION_READY
        assert len(assessment.blocking_findings) > 0 or len(assessment.critical_findings) > 0
        # CP-024 checkpoint reflects failure
        cp024 = results.get("CP-024")
        assert cp024 is not None
        assert cp024.status.value == "FAIL"

    def test_scenario_c_strict_policy_missing_runtime_insufficient_evidence(
        self, contracts_dir: Path, code_dir: Path
    ) -> None:
        contract = load_contract_file(str(contracts_dir / "customer_daily_pipeline.yaml"))
        code_text = (code_dir / "good_pipeline.py").read_text(encoding="utf-8")
        cluster_config, job_config = contract_cluster_job(contract)

        cps = build_all_checkpoints(
            contract=contract,
            data_profile=None,
            code_text=code_text,
            cluster_config=cluster_config,
            job_config=job_config,
            runtime_data=None,
        )
        policy = ReadinessPolicy(require_runtime_evidence=True)
        context = {
            "pipeline_contract": contract,
            "code_snippet": code_text,
            "cluster_config": cluster_config,
            "job_config": job_config,
            "runtime_data": None,
            "readiness_policy": policy,
        }
        engine = CheckpointEngine()
        engine.run_all_checkpoints(cps, context)

        assessment = context.get("production_readiness_assessment")
        assert assessment is not None
        assert assessment.status == ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
        assert any("runtime" in r.lower() for r in assessment.decision_reasons)

    def test_scenario_d_pipeline_with_warnings(
        self, contracts_dir: Path, metadata_dir: Path, code_dir: Path
    ) -> None:
        contract = load_contract_file(str(contracts_dir / "customer_daily_pipeline.yaml"))
        profile = _load_profile(metadata_dir / "profile_500gb.json")
        contract.source.data_profile = profile
        code_text = (code_dir / "good_pipeline.py").read_text(encoding="utf-8")
        cluster_config, job_config = contract_cluster_job(contract)

        cps = build_all_checkpoints(
            contract=contract,
            data_profile=profile,
            code_text=code_text,
            cluster_config=cluster_config,
            job_config=job_config,
        )
        policy = ReadinessPolicy(minimum_quality_score=60.0, minimum_evidence_coverage=40.0)
        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": contract.source,
            "data_profile": profile,
            "code_snippet": code_text,
            "cluster_config": cluster_config,
            "job_config": job_config,
            "readiness_policy": policy,
            "rule_context": {
                "data_size_gb": profile.total_gb,
                "source_type": contract.source.type.value,
                "workload_type": contract.processing,
            },
        }
        engine = CheckpointEngine()
        engine.run_all_checkpoints(cps, context)

        assessment = context.get("production_readiness_assessment")
        assert assessment is not None
        assert assessment.status == ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS
        assert assessment.warning_count > 0

    def test_scenario_e_cluster_config_drift_detection(
        self, contracts_dir: Path, code_dir: Path
    ) -> None:
        contract = load_contract_file(str(contracts_dir / "customer_daily_pipeline.yaml"))
        cluster_config, job_config = contract_cluster_job(contract)
        actual_env = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
        }

        cps = build_all_checkpoints(contract=contract, cluster_config=cluster_config)
        context = {
            "pipeline_contract": contract,
            "cluster_config": cluster_config,
            "job_config": job_config,
            "actual_environment": actual_env,
        }
        engine = CheckpointEngine()
        engine.run_all_checkpoints(cps, context)

        assessment = context.get("production_readiness_assessment")
        assert assessment is not None
        assert len(assessment.config_comparisons) > 0
        diff_dbr = next(
            (c for c in assessment.config_comparisons if c.parameter == "Spark/DBR Version"), None
        )
        assert diff_dbr is not None
        if diff_dbr.expected and diff_dbr.actual:
            assert diff_dbr.is_drift is True

    def test_scenario_f_projected_sla_decoupled_from_observed_sla(
        self, contracts_dir: Path, code_dir: Path
    ) -> None:
        contract = load_contract_file(str(contracts_dir / "customer_daily_pipeline.yaml"))
        cluster_config, job_config = contract_cluster_job(contract)
        cps = build_all_checkpoints(contract=contract, cluster_config=cluster_config)
        context = {
            "pipeline_contract": contract,
            "cluster_config": cluster_config,
            "job_config": job_config,
        }
        engine = CheckpointEngine()
        results = engine.run_all_checkpoints(cps, context)

        assessment = context.get("production_readiness_assessment")
        assert assessment is not None
        cp023 = results.get("CP-023")
        assert cp023 is not None
        assert cp023.status.value == "UNKNOWN"

    def test_secret_redaction_in_evidence_and_findings(
        self, contracts_dir: Path
    ) -> None:
        contract_path = str(contracts_dir / "customer_daily_pipeline.yaml")
        runner = CliRunner()
        res = runner.invoke(
            cli,
            ["validate", "--contract", contract_path, "--offline"],
        )
        assert res.exit_code == 0
        assert "secret_token_123456789" not in res.output

    def test_cli_json_output_schema(
        self, contracts_dir: Path
    ) -> None:
        contract_path = str(contracts_dir / "customer_daily_pipeline.yaml")
        runner = CliRunner()
        res = runner.invoke(
            cli,
            ["validate", "--contract", contract_path, "--offline", "--json"],
        )
        assert res.exit_code == 0
        payload = json.loads(res.output)
        assert "status" in payload
        assert "quality_score" in payload
        assert "evidence_coverage" in payload
        assert "comprehensive_coverage" in payload
        assert "decision_reasons" in payload
        assert "required_actions" in payload
        assert "config_comparisons" in payload
        assert "cross_domain_risks" in payload
        assert "checkpoints" in payload
        assert payload["status"] in [
            "PRODUCTION_READY",
            "PRODUCTION_READY_WITH_WARNINGS",
            "NOT_PRODUCTION_READY",
            "INSUFFICIENT_EVIDENCE",
        ]
