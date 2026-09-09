"""Integration tests for Phase 6 Databricks Environment Intelligence.

Validates end-to-end flow:
Contract + Data Profile + Source Code + Databricks Cluster + Databricks Job.
Tests Checkpoint CP-009 (Cluster Validation) and CP-011 (Job Validation).
Tests strict UNKNOWN semantics when Databricks environment data is missing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.connectors.offline import OfflineDatabricksConnector
from dpif.contract.loader import load_contract_file
from dpif.models import (
    AnalysisMethod,
    CheckpointStatus,
    CollectionMethod,
    DataProfile,
    SchemaColumn,
)


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


@pytest.fixture
def fixtures_dir(repo_root) -> Path:
    return repo_root / "tests" / "fixtures"


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
        record_count=int(raw.get("record_count", 0)),
        schema_columns=columns,
        analysis_method=AnalysisMethod.METADATA,
        collection_method=CollectionMethod.FIXTURE,
        evidence_source="fixture metadata",
    )


class TestDatabricksEnvironmentIntegration:
    def test_good_pipeline_with_good_databricks_env_passes(self, fixtures_dir):
        contract = load_contract_file(fixtures_dir / "contracts" / "healthy_parquet_pipeline.yaml")
        profile = _load_profile(fixtures_dir / "metadata" / "profile_healthy_parquet.json")
        contract.source.data_profile = profile
        code_text = (fixtures_dir / "code" / "good_pipeline.py").read_text(encoding="utf-8")

        conn = OfflineDatabricksConnector(fixtures_dir / "databricks")
        job = conn.get_job("job_good.json")
        cluster = conn.get_cluster("cluster_good.json")
        policy = conn.get_cluster_policy("policy_standard.json")

        assert job is not None
        assert cluster is not None

        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": contract.source,
            "data_profile": profile,
            "code_snippet": code_text,
            "code_filename": "good_pipeline.py",
            "cluster_config": cluster,
            "cluster": cluster,
            "cluster_policy": policy,
            "job_config": job,
            "job": job,
            "assumptions": {"mode": "offline-fixture"},
            "rule_context": {
                "data_size_gb": profile.total_gb,
                "source_type": contract.source.type.value,
                "workload_type": contract.processing,
            },
        }

        cps = build_all_checkpoints(contract, profile, code_text, cluster, job)
        results = CheckpointEngine().run_all_checkpoints(cps, context)

        # CP-009 Cluster Validation and CP-011 Job Validation must pass with no FAIL findings
        assert results["CP-009"].status == CheckpointStatus.PASS
        assert not any(f.status == CheckpointStatus.FAIL for f in results["CP-009"].findings)

        assert results["CP-011"].status == CheckpointStatus.PASS
        assert not any(f.status == CheckpointStatus.FAIL for f in results["CP-011"].findings)

    def test_bad_pipeline_with_bad_databricks_env_fails(self, fixtures_dir):
        contract = load_contract_file(fixtures_dir / "contracts" / "bad_production_pipeline.yaml")
        profile = _load_profile(fixtures_dir / "metadata" / "profile_500gb.json")
        contract.source.data_profile = profile
        code_text = (fixtures_dir / "code" / "bad_pipeline.py").read_text(encoding="utf-8")

        conn = OfflineDatabricksConnector(fixtures_dir / "databricks")
        job = conn.get_job("job_bad.json")
        cluster = conn.get_cluster("cluster_bad.json")

        assert job is not None
        assert cluster is not None

        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": contract.source,
            "data_profile": profile,
            "code_snippet": code_text,
            "code_filename": "bad_pipeline.py",
            "cluster_config": cluster,
            "cluster": cluster,
            "job_config": job,
            "job": job,
            "assumptions": {"mode": "offline-fixture"},
            "rule_context": {
                "data_size_gb": profile.total_gb,
                "source_type": contract.source.type.value,
                "workload_type": contract.processing,
            },
        }

        cps = build_all_checkpoints(contract, profile, code_text, cluster, job)
        results = CheckpointEngine().run_all_checkpoints(cps, context)

        # Cluster has invalid autoscaling range (10 > 2) -> CP-009 must FAIL
        assert results["CP-009"].status == CheckpointStatus.FAIL
        cluster_rules = {f.rule_id for f in results["CP-009"].findings}
        assert "CONFIG-CLUSTER-003" in cluster_rules or "CONFIG-CLUSTER-004" in cluster_rules

        # Job has 0 retries, no timeout -> CP-011 warns, CP-014 fails on retries
        assert results["CP-011"].status == CheckpointStatus.WARN
        job_rules = {f.rule_id for f in results["CP-011"].findings}
        assert "CONFIG-JOB-001" in job_rules or "CONFIG-JOB-002" in job_rules
        assert results["CP-014"].status == CheckpointStatus.FAIL

    def test_outdated_dbr_cluster_fails_cluster_validation(self, fixtures_dir):
        contract = load_contract_file(fixtures_dir / "contracts" / "healthy_parquet_pipeline.yaml")
        profile = _load_profile(fixtures_dir / "metadata" / "profile_healthy_parquet.json")
        contract.source.data_profile = profile
        code_text = (fixtures_dir / "code" / "good_pipeline.py").read_text(encoding="utf-8")

        conn = OfflineDatabricksConnector(fixtures_dir / "databricks")
        outdated_cluster = conn.get_cluster("cluster_outdated_dbr.json")
        job = conn.get_job("job_good.json")

        assert outdated_cluster is not None
        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": contract.source,
            "data_profile": profile,
            "code_snippet": code_text,
            "code_filename": "good_pipeline.py",
            "cluster_config": outdated_cluster,
            "cluster": outdated_cluster,
            "job_config": job,
            "job": job,
            "assumptions": {"mode": "offline-fixture"},
            "rule_context": {"data_size_gb": profile.total_gb},
        }

        cps = build_all_checkpoints(contract, profile, code_text, outdated_cluster, job)
        results = CheckpointEngine().run_all_checkpoints(cps, context)

        assert results["CP-009"].status == CheckpointStatus.FAIL
        cluster_rules = {f.rule_id for f in results["CP-009"].findings}
        assert "CONFIG-CLUSTER-001" in cluster_rules

    def test_missing_environment_preserves_strict_unknown_semantics(self, fixtures_dir):
        """When Databricks environment data is absent, CP-009 & CP-011 report UNKNOWN."""
        contract = load_contract_file(fixtures_dir / "contracts" / "healthy_parquet_pipeline.yaml")
        profile = _load_profile(fixtures_dir / "metadata" / "profile_healthy_parquet.json")
        contract.source.data_profile = profile
        code_text = (fixtures_dir / "code" / "good_pipeline.py").read_text(encoding="utf-8")

        # Null cluster and null job
        context = {
            "pipeline_name": contract.pipeline_name,
            "pipeline_contract": contract,
            "source": contract.source,
            "data_profile": profile,
            "code_snippet": code_text,
            "code_filename": "good_pipeline.py",
            "cluster_config": None,
            "job_config": None,
            "cluster": None,
            "job": None,
            "assumptions": {"mode": "no-databricks-connection"},
            "rule_context": {"data_size_gb": profile.total_gb},
        }

        cps = build_all_checkpoints(contract, profile, code_text, None, None)
        results = CheckpointEngine().run_all_checkpoints(cps, context)

        # CP-009 and CP-011 must be UNKNOWN, not PASS and not FAIL
        assert results["CP-009"].status == CheckpointStatus.UNKNOWN
        assert results["CP-011"].status == CheckpointStatus.UNKNOWN
        assert results["CP-009"].score == 0.0 or results["CP-009"].score == 0.5
        assert results["CP-011"].score == 0.0 or results["CP-011"].score == 0.5
        assert (
            "No" in results["CP-009"].evidence.recommendation
            or "No cluster" in results["CP-009"].description
        )
        assert (
            "No" in results["CP-011"].evidence.recommendation
            or "No job" in results["CP-011"].description
        )
