"""Unit tests verifying cross-phase reasoning for Phase 8 Scalability Intelligence.

Ensures that:
- Phase 3 Data Profile informs file/partition growth projections.
- Phase 4 PySpark AST informs driver collection scalability risks.
- Phase 5 SQL analysis informs Cartesian cross-join and aggregation scalability.
- Phase 6 Databricks Cluster configurations inform compute headroom and autoscaling elasticity.
- Phase 7 Runtime Telemetry informs shuffle scaling and SLA violation risks.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpif.code.parser import analyze_source
from dpif.models import (
    AnalysisMethod,
    CheckpointStatus,
    CollectionMethod,
    DataProfile,
    PipelineContract,
    Rule,
    Source,
    SourceType,
    Target,
)
from dpif.rules.engine import load_rules
from dpif.runtime.models import RuntimeRun, RuntimeStage
from dpif.scalability.models import (
    ScalabilityObservation,
)
from dpif.sql.parser import parse_sql


@pytest.fixture(scope="module")
def scalability_rules() -> dict[str, Rule]:
    all_rules = load_rules()
    return {r.rule_id: r for r in all_rules if r.rule_id.startswith("SCALABILITY-")}


def _contract_with_scales(
    expected_gb: float = 100.0,
    peak_gb: float = 400.0,
    sla_min: float = 60.0,
) -> PipelineContract:
    c = PipelineContract(
        contract_id="c-cross",
        pipeline_name="cross_phase_pipeline",
        source=Source(
            source_id="s1",
            type=SourceType.DELTA,
            path="abfss://lake@acc.dfs.core.windows.net/data",
        ),
        target=Target(target_id="t1", type="delta", path="/mnt/delta/out"),
        expected_daily_volume_gb=expected_gb,
        peak_daily_volume_gb=peak_gb,
        growth_rate_percent=20.0,
    )
    c.sla.max_runtime_minutes = sla_min
    return c


class TestCrossPhaseScalability:
    def test_phase3_profile_informs_file_count_growth(self, scalability_rules):
        """Phase 3 DataProfile with small files projected under Phase 8 4x scaling."""
        rule = scalability_rules["SCALABILITY-004"]
        contract = _contract_with_scales(expected_gb=100.0, peak_gb=400.0)
        profile = DataProfile(
            total_bytes=100 * (1024**3),
            total_gb=100.0,
            file_count=20000,
            average_file_size_kb=5120.0,  # 5 MB average size
            median_file_size_kb=5120.0,
            p95_file_size_kb=5120.0,
            p99_file_size_kb=5120.0,
            min_file_size_kb=5120.0,
            max_file_size_kb=5120.0,
            record_count=1000000,
            partition_count=10,
            column_count=10,
            analysis_method=AnalysisMethod.METADATA,
            collection_method=CollectionMethod.FIXTURE,
            evidence_source="phase3_metadata",
            compression="snappy",
        )
        finding = rule.evaluate({"contract": contract, "data_profile": profile})
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN
        assert finding.evidence.observed["projected_file_count"] == 80000  # 20k * 4x

    def test_phase4_ast_informs_driver_scalability(self, scalability_rules):
        """Phase 4 PySpark AST parser output combined with Phase 8 workload volume."""
        rule = scalability_rules["SCALABILITY-006"]
        contract = _contract_with_scales(expected_gb=200.0, peak_gb=500.0)
        code = (
            "df = spark.read.parquet('/path')\n"
            "driver_records = df.collect()\n"
        )
        ast_result = analyze_source(code, filename="pipeline.py")
        finding = rule.evaluate({"contract": contract, "code_analysis": ast_result})
        assert finding is not None
        assert finding.status == CheckpointStatus.FAIL
        assert finding.evidence.observed["driver_materializations"] >= 1
        assert finding.evidence.observed["workload_volume_gb"] == 200.0

    def test_phase5_sql_informs_cross_join_scalability(self, scalability_rules):
        """Phase 5 SQL parser queries combined with Phase 8 workload volume."""
        rule = scalability_rules["SCALABILITY-008"]
        contract = _contract_with_scales(expected_gb=100.0, peak_gb=300.0)
        sql = "SELECT * FROM large_orders CROSS JOIN large_customers"
        sql_analysis = parse_sql(sql)
        finding = rule.evaluate({"contract": contract, "sql_analysis": sql_analysis})
        assert finding is not None
        assert finding.status == CheckpointStatus.FAIL
        assert finding.evidence.observed["cross_join_detected"] is True

    def test_phase6_cluster_informs_autoscaling_headroom(self, scalability_rules):
        """Phase 6 cluster configuration combined with Phase 8 4x peak surge."""
        rule = scalability_rules["SCALABILITY-011"]
        contract = _contract_with_scales(expected_gb=100.0, peak_gb=400.0)  # 4x surge
        cluster = {"num_workers": 4}  # fixed cluster, no autoscaling
        finding = rule.evaluate({"contract": contract, "cluster_config": cluster})
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN
        assert "autoscaling" in finding.recommendation.lower()

    def test_phase7_runtime_informs_shuffle_scalability(self, scalability_rules):
        """Phase 7 runtime shuffle telemetry projected under Phase 8 5x peak volume."""
        rule = scalability_rules["SCALABILITY-007"]
        contract = _contract_with_scales(expected_gb=100.0, peak_gb=500.0)  # 5x scaling
        stage = RuntimeStage(stage_id=1, name="Shuffle", shuffle_read_bytes=50 * (1024**3))
        run = RuntimeRun(run_id="r1", stages=[stage])
        finding = rule.evaluate({"contract": contract, "runtime_run": run})
        assert finding is not None
        assert finding.status == CheckpointStatus.WARN
        assert finding.evidence.observed["projected_shuffle_gb"] == 250.0  # 50 * 5x

    def test_historical_runs_fixture_trend_detection(self, scalability_rules):
        """Multi-run historical fixtures tested against SCALABILITY-013 and SCALABILITY-014."""
        fixture_path = Path("tests/fixtures/scalability/historical_superlinear_runs.json")
        raw_runs = json.loads(fixture_path.read_text(encoding="utf-8"))
        observations = [
            ScalabilityObservation(
                run_id=r["run_id"],
                volume_gb=r["input_volume_gb"],
                duration_minutes=r["duration_minutes"],
                shuffle_bytes=int(r.get("shuffle_read_gb", 0) * (1024**3)),
                spill_bytes=int(r.get("spill_to_disk_gb", 0) * (1024**3)),
                task_count=100,
                failed_tasks=0 if r.get("success", True) else 15,
            )
            for r in raw_runs
        ]

        # SCALABILITY-013: Runtime Regression at Scale
        r13 = scalability_rules["SCALABILITY-013"].evaluate({"historical_runs": observations})
        assert r13 is not None
        assert r13.status == CheckpointStatus.WARN

        # SCALABILITY-014: Failure Escalation at Scale
        r14 = scalability_rules["SCALABILITY-014"].evaluate({"historical_runs": observations})
        assert r14 is not None
        assert r14.status == CheckpointStatus.WARN
