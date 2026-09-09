"""End-to-end integration tests for CP-010 Scalability Validation, scoring, and CLI."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.cli import cli
from dpif.contract.loader import contract_cluster_job, load_contract_file
from dpif.models import (
    AnalysisMethod,
    CheckpointStatus,
    CollectionMethod,
    DataProfile,
    SchemaColumn,
)
from dpif.scalability.models import ScalabilityObservation
from dpif.scoring.engine import score_checkpoints


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
        schema_columns=columns,
    )


class TestScalabilityValidationIntegration:
    def test_cp010_with_linear_pipeline_passes_or_warns_cleanly(self, fixtures_dir):
        contract_path = fixtures_dir / "contracts" / "customer_daily_pipeline.yaml"
        profile_path = fixtures_dir / "metadata" / "profile_500gb.json"
        code_path = fixtures_dir / "code" / "good_pipeline.py"
        linear_runs_path = fixtures_dir / "scalability" / "historical_linear_runs.json"

        contract = load_contract_file(contract_path)
        profile = _load_profile(profile_path)
        contract.source.data_profile = profile
        code_text = code_path.read_text(encoding="utf-8")
        cluster_config, job_config = contract_cluster_job(contract)

        raw_runs = json.loads(linear_runs_path.read_text(encoding="utf-8"))
        observations = [
            ScalabilityObservation(
                run_id=r["run_id"],
                volume_gb=r["input_volume_gb"],
                duration_minutes=r["duration_minutes"],
                shuffle_bytes=int(r.get("shuffle_read_gb", 0) * (1024**3)),
                task_count=100,
                failed_tasks=0,
            )
            for r in raw_runs
        ]

        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": contract.source,
            "data_profile": profile,
            "code_snippet": code_text,
            "code_filename": code_path.name,
            "cluster_config": cluster_config,
            "job_config": job_config,
            "historical_runs": observations,
            "assumptions": {"mode": "offline-fixture"},
            "rule_context": {
                "data_size_gb": profile.total_gb,
                "source_type": contract.source.type.value,
                "workload_type": contract.processing,
            },
        }

        cps = build_all_checkpoints(
            contract,
            profile,
            code_text,
            cluster_config,
            job_config,
            historical_runs=observations,
        )
        cp010_def = next((cp for cp in cps if cp.checkpoint_id == "CP-010"), None)
        assert cp010_def is not None
        assert cp010_def.category == "scalability"

        engine = CheckpointEngine()
        results = engine.run_all_checkpoints(cps, context)

        cp010_result = results["CP-010"]
        assert cp010_result is not None
        # Good pipeline has no blocking failures in CP-010
        assert cp010_result.status != CheckpointStatus.FAIL

        # Verify scoring includes scalability category
        score, _ = score_checkpoints(results)
        assert "scalability" in score.categories

    def test_cp010_with_bad_pipeline_and_superlinear_runs_fails(self, fixtures_dir):
        contract_path = fixtures_dir / "contracts" / "bad_production_pipeline.yaml"
        profile_path = fixtures_dir / "metadata" / "profile_500gb.json"
        code_path = fixtures_dir / "code" / "bad_pipeline.py"
        superlinear_runs_path = fixtures_dir / "scalability" / "historical_superlinear_runs.json"

        contract = load_contract_file(contract_path)
        profile = _load_profile(profile_path)
        contract.source.data_profile = profile
        code_text = code_path.read_text(encoding="utf-8")
        cluster_config, job_config = contract_cluster_job(contract)

        raw_runs = json.loads(superlinear_runs_path.read_text(encoding="utf-8"))
        observations = [
            ScalabilityObservation(
                run_id=r["run_id"],
                volume_gb=r["input_volume_gb"],
                duration_minutes=r["duration_minutes"],
                shuffle_bytes=int(r.get("shuffle_read_gb", 0) * (1024**3)),
                task_count=100,
                failed_tasks=15 if not r.get("success", True) else 0,
            )
            for r in raw_runs
        ]

        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": contract.source,
            "data_profile": profile,
            "code_snippet": code_text,
            "code_filename": code_path.name,
            "cluster_config": cluster_config,
            "job_config": job_config,
            "historical_runs": observations,
            "assumptions": {"mode": "offline-fixture"},
            "rule_context": {
                "data_size_gb": profile.total_gb,
                "source_type": contract.source.type.value,
                "workload_type": contract.processing,
            },
        }

        cps = build_all_checkpoints(
            contract,
            profile,
            code_text,
            cluster_config,
            job_config,
            historical_runs=observations,
        )
        engine = CheckpointEngine()
        results = engine.run_all_checkpoints(cps, context)

        cp010_result = results["CP-010"]
        assert cp010_result.status == CheckpointStatus.FAIL

        rule_ids = {f.rule_id for f in cp010_result.findings}
        # Driver collection at 500 GB workload fails SCALABILITY-006
        assert "SCALABILITY-006" in rule_ids
        # Superlinear runs trigger SCALABILITY-013
        assert "SCALABILITY-013" in rule_ids

    def test_cp010_without_telemetry_preserves_strict_unknown(self, fixtures_dir):
        contract_path = fixtures_dir / "contracts" / "customer_daily_pipeline.yaml"
        code_path = fixtures_dir / "code" / "good_pipeline.py"

        contract = load_contract_file(contract_path)
        code_text = code_path.read_text(encoding="utf-8")

        # Completely omit data_profile, cluster_config, and historical_runs
        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "code_snippet": code_text,
        }

        cps = build_all_checkpoints(contract, None, code_text, None, None, historical_runs=None)
        engine = CheckpointEngine()
        results = engine.run_all_checkpoints(cps, context)

        cp010_result = results["CP-010"]
        unknown_findings = [
            f for f in cp010_result.findings if f.status == CheckpointStatus.UNKNOWN
        ]
        assert len(unknown_findings) > 0
        for f in unknown_findings:
            assert f.confidence == 0.0

    def test_cli_validate_with_historical_runs(self, fixtures_dir):
        contract = str(fixtures_dir / "contracts" / "customer_daily_pipeline.yaml")
        profile = str(fixtures_dir / "metadata" / "profile_500gb.json")
        code = str(fixtures_dir / "code" / "good_pipeline.py")
        runs = str(fixtures_dir / "scalability" / "historical_linear_runs.json")

        runner = CliRunner()
        res = runner.invoke(
            cli,
            [
                "validate",
                "--contract",
                contract,
                "--data-profile",
                profile,
                "--code",
                code,
                "--historical-runs",
                runs,
            ],
        )
        assert res.exit_code == 0
        assert "Scalability" in res.output
        assert "SCALABILITY INTELLIGENCE" in res.output
        assert "LINEAR" in res.output
