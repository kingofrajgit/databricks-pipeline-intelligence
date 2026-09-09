"""Comprehensive unit tests for all 14 Phase 8 Scalability Rules (SCALABILITY-001..014).

Covers:
- Positive (WARN and/or FAIL triggers)
- Negative (scalable / compliant pipelines)
- Edge cases
- Threshold overrides
- Strict UNKNOWN semantics on missing context / telemetry
"""

from __future__ import annotations

import pytest

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


@pytest.fixture(scope="module")
def scalability_rules() -> dict[str, Rule]:
    all_rules = load_rules()
    return {r.rule_id: r for r in all_rules if r.rule_id.startswith("SCALABILITY-")}


def _make_contract(
    expected_gb: float = 100.0,
    peak_gb: float = 250.0,
    growth_rate_pct: float = 15.0,
    sla_runtime_min: float = 60.0,
) -> PipelineContract:
    c = PipelineContract(
        contract_id="contract-test",
        pipeline_name="test_pipeline",
        source=Source(
            source_id="s1",
            type=SourceType.DELTA,
            path="abfss://test@acc.dfs.core.windows.net/data",
        ),
        target=Target(target_id="t1", type="delta", path="/mnt/delta/test"),
        expected_daily_volume_gb=expected_gb,
        peak_daily_volume_gb=peak_gb,
        growth_rate_percent=growth_rate_pct,
    )
    c.sla.max_runtime_minutes = sla_runtime_min
    return c


def _make_profile(
    total_gb: float = 100.0,
    file_count: int = 1000,
    partition_count: int = 10,
    avg_file_size_kb: float = 102400.0,  # 100 MB
    partition_sizes_gb: dict[str, float] | None = None,
) -> DataProfile:
    return DataProfile(
        total_bytes=int(total_gb * (1024**3)),
        total_gb=total_gb,
        file_count=file_count,
        average_file_size_kb=avg_file_size_kb,
        median_file_size_kb=avg_file_size_kb,
        p95_file_size_kb=avg_file_size_kb,
        p99_file_size_kb=avg_file_size_kb,
        min_file_size_kb=avg_file_size_kb,
        max_file_size_kb=avg_file_size_kb,
        record_count=1000000,
        partition_count=partition_count,
        partition_distribution={"p0": 1000},
        column_count=10,
        analysis_method=AnalysisMethod.METADATA,
        collection_method=CollectionMethod.FIXTURE,
        evidence_source="fixture",
        compression="snappy",
        partition_sizes_gb=partition_sizes_gb or {},
    )


# ====================================================================
# SCALABILITY-001: Expected Volume Capacity Risk
# ====================================================================
def test_sc_001_warn(scalability_rules):
    rule = scalability_rules["SCALABILITY-001"]
    # Expected daily volume is 400 GB, verified profile is 100 GB -> 4.0x gap > 2.0x
    contract = _make_contract(expected_gb=400.0)
    profile = _make_profile(total_gb=100.0)
    finding = rule.evaluate({"contract": contract, "profile": profile})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN
    assert "4.0x" in finding.evidence.evidence[0]


def test_sc_001_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-001"]
    contract = _make_contract(expected_gb=150.0)
    profile = _make_profile(total_gb=100.0)  # 1.5x gap <= 2.0x
    finding = rule.evaluate({"contract": contract, "profile": profile})
    assert finding is None


def test_sc_001_edge_exact_boundary(scalability_rules):
    rule = scalability_rules["SCALABILITY-001"]
    contract = _make_contract(expected_gb=200.0)
    profile = _make_profile(total_gb=100.0)  # exactly 2.0x gap
    finding = rule.evaluate({"contract": contract, "profile": profile})
    assert finding is None


def test_sc_001_threshold_override(scalability_rules):
    rule = scalability_rules["SCALABILITY-001"].model_copy(
        update={"params": {"warn_ratio": 1.2}}
    )
    contract = _make_contract(expected_gb=150.0)
    profile = _make_profile(total_gb=100.0)  # 1.5x > 1.2
    finding = rule.evaluate({"contract": contract, "profile": profile})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_001_unknown_missing_evidence(scalability_rules):
    rule = scalability_rules["SCALABILITY-001"]
    contract = _make_contract(expected_gb=400.0)
    finding = rule.evaluate({"contract": contract})  # No profile or runtime
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN
    assert finding.confidence == 0.0


# ====================================================================
# SCALABILITY-002: Peak Volume Capacity Risk
# ====================================================================
def test_sc_002_warn(scalability_rules):
    rule = scalability_rules["SCALABILITY-002"]
    contract = _make_contract(expected_gb=100.0, peak_gb=300.0)  # 3.0x surge > 2.5x
    finding = rule.evaluate({"contract": contract})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_002_fail(scalability_rules):
    rule = scalability_rules["SCALABILITY-002"]
    contract = _make_contract(expected_gb=100.0, peak_gb=1200.0)  # 12.0x surge > 10.0x
    finding = rule.evaluate({"contract": contract})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_sc_002_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-002"]
    contract = _make_contract(expected_gb=100.0, peak_gb=150.0)  # 1.5x surge
    finding = rule.evaluate({"contract": contract})
    assert finding is None


def test_sc_002_override(scalability_rules):
    rule = scalability_rules["SCALABILITY-002"].model_copy(
        update={"params": {"warn_ratio": 1.5}}
    )
    contract = _make_contract(expected_gb=100.0, peak_gb=180.0)
    finding = rule.evaluate({"contract": contract})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_002_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-002"]
    finding = rule.evaluate({})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-003: Data Growth Risk
# ====================================================================
def test_sc_003_warn(scalability_rules):
    rule = scalability_rules["SCALABILITY-003"]
    contract = _make_contract(growth_rate_pct=25.0)  # 25% > 20%
    finding = rule.evaluate({"contract": contract})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_003_fail(scalability_rules):
    rule = scalability_rules["SCALABILITY-003"]
    contract = _make_contract(growth_rate_pct=55.0)  # 55% > 50%
    finding = rule.evaluate({"contract": contract})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_sc_003_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-003"]
    contract = _make_contract(growth_rate_pct=10.0)  # 10% <= 20%
    finding = rule.evaluate({"contract": contract})
    assert finding is None


def test_sc_003_override(scalability_rules):
    rule = scalability_rules["SCALABILITY-003"].model_copy(
        update={"params": {"warn_growth_rate": 5.0}}
    )
    contract = _make_contract(growth_rate_pct=8.0)
    finding = rule.evaluate({"contract": contract})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_003_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-003"]
    finding = rule.evaluate({})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-004: File Count Growth Risk
# ====================================================================
def test_sc_004_warn(scalability_rules):
    rule = scalability_rules["SCALABILITY-004"]
    contract = _make_contract(expected_gb=100.0, peak_gb=500.0)  # 5x scaling
    profile = _make_profile(
        total_gb=100.0,
        file_count=20000,
        avg_file_size_kb=5120.0,  # 5 MB avg size, projected to 100,000 files > 50,000
    )
    finding = rule.evaluate({"contract": contract, "data_profile": profile})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_004_fail(scalability_rules):
    rule = scalability_rules["SCALABILITY-004"]
    contract = _make_contract(expected_gb=100.0, peak_gb=500.0)
    profile = _make_profile(
        total_gb=100.0,
        file_count=50000,
        avg_file_size_kb=2048.0,  # 2 MB avg size, projected to 250,000 files > 200,000
    )
    finding = rule.evaluate({"contract": contract, "data_profile": profile})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_sc_004_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-004"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    profile = _make_profile(
        total_gb=100.0,
        file_count=500,
        avg_file_size_kb=204800.0,  # 200 MB files
    )
    finding = rule.evaluate({"contract": contract, "data_profile": profile})
    assert finding is None


def test_sc_004_override(scalability_rules):
    rule = scalability_rules["SCALABILITY-004"].model_copy(
        update={"params": {"warn_file_count": 1000}}
    )
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    profile = _make_profile(total_gb=100.0, file_count=800, avg_file_size_kb=1024.0)
    finding = rule.evaluate({"contract": contract, "data_profile": profile})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_004_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-004"]
    finding = rule.evaluate({})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-005: Partition Scalability Risk
# ====================================================================
def test_sc_005_warn_count(scalability_rules):
    rule = scalability_rules["SCALABILITY-005"]
    contract = _make_contract(expected_gb=100.0, peak_gb=100.0)
    profile = _make_profile(total_gb=100.0, file_count=12000, partition_count=12000)
    finding = rule.evaluate({"contract": contract, "data_profile": profile})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_005_warn_skew(scalability_rules):
    rule = scalability_rules["SCALABILITY-005"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    sizes = {"p1": 90.0}
    for i in range(2, 11):
        sizes[f"p{i}"] = 1.0
    profile = _make_profile(
        total_gb=99.0,
        file_count=1000,
        partition_count=10,
        partition_sizes_gb=sizes,
    )
    finding = rule.evaluate({"contract": contract, "data_profile": profile})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_005_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-005"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    profile = _make_profile(
        total_gb=100.0,
        file_count=500,
        partition_count=20,
        partition_sizes_gb={"p1": 5.0, "p2": 5.0, "p3": 5.0},
    )
    finding = rule.evaluate({"contract": contract, "data_profile": profile})
    assert finding is None


def test_sc_005_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-005"]
    finding = rule.evaluate({})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-006: Driver Scalability Risk
# ====================================================================
def test_sc_006_fail_collect(scalability_rules):
    rule = scalability_rules["SCALABILITY-006"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    code = "rows = df.collect()"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_sc_006_fail_topandas(scalability_rules):
    rule = scalability_rules["SCALABILITY-006"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    code = "pdf = df.toPandas()"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_sc_006_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-006"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    code = "df.write.format('delta').save('/path')"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is None


def test_sc_006_small_volume_warn(scalability_rules):
    rule = scalability_rules["SCALABILITY-006"]
    contract = _make_contract(expected_gb=5.0, peak_gb=10.0)  # Under 50 GB threshold -> WARN
    code = "rows = df.collect()"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_006_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-006"]
    finding = rule.evaluate({})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-007: Shuffle Scalability Risk
# ====================================================================
def test_sc_007_warn(scalability_rules):
    rule = scalability_rules["SCALABILITY-007"]
    contract = _make_contract(expected_gb=100.0, peak_gb=500.0)  # 5x scaling
    stage = RuntimeStage(stage_id=1, name="Shuffle", shuffle_read_bytes=60 * (1024**3))  # 60 GB
    run = RuntimeRun(run_id="r1", stages=[stage])  # projected to 300 GB > 200 GB
    finding = rule.evaluate({"contract": contract, "runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_007_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-007"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)  # 2x scaling
    stage = RuntimeStage(stage_id=1, name="Shuffle", shuffle_read_bytes=10 * (1024**3))  # 10 GB
    run = RuntimeRun(run_id="r1", stages=[stage])  # projected to 20 GB <= 200 GB
    finding = rule.evaluate({"contract": contract, "runtime_run": run})
    assert finding is None


def test_sc_007_override(scalability_rules):
    rule = scalability_rules["SCALABILITY-007"].model_copy(
        update={"params": {"warn_projected_shuffle_gb": 50.0}}
    )
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    stage = RuntimeStage(stage_id=1, name="Shuffle", shuffle_read_bytes=30 * (1024**3))
    run = RuntimeRun(run_id="r1", stages=[stage])  # 60 GB > 50 GB
    finding = rule.evaluate({"contract": contract, "runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_007_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-007"]
    contract = _make_contract(expected_gb=100.0, peak_gb=500.0)
    finding = rule.evaluate({"contract": contract})  # No runtime
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-008: Join Scalability Risk
# ====================================================================
def test_sc_008_fail_cross_join(scalability_rules):
    rule = scalability_rules["SCALABILITY-008"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    code = "res = df1.crossJoin(df2)"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is not None
    assert finding.status == CheckpointStatus.FAIL


def test_sc_008_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-008"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0)
    code = "res = df1.join(df2, on='id')"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is None


def test_sc_008_negative_clean_code(scalability_rules):
    rule = scalability_rules["SCALABILITY-008"]
    code = "df.write.parquet('/path')"
    finding = rule.evaluate({"code_snippet": code})
    assert finding is None


# ====================================================================
# SCALABILITY-009: Aggregation Scalability Risk
# ====================================================================
def test_sc_009_warn_distinct(scalability_rules):
    rule = scalability_rules["SCALABILITY-009"]
    contract = _make_contract(expected_gb=200.0, peak_gb=400.0)  # >= 100 GB
    code = "res = df.select('customer_id').distinct()"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_009_negative_small_volume(scalability_rules):
    rule = scalability_rules["SCALABILITY-009"]
    contract = _make_contract(expected_gb=10.0, peak_gb=20.0)  # < 100 GB
    code = "res = df.select('customer_id').distinct()"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is None


def test_sc_009_negative_groupby(scalability_rules):
    rule = scalability_rules["SCALABILITY-009"]
    contract = _make_contract(expected_gb=200.0, peak_gb=400.0)
    code = "res = df.groupBy('category').count()"
    finding = rule.evaluate({"contract": contract, "code_snippet": code})
    assert finding is None


# ====================================================================
# SCALABILITY-010: Cluster Capacity Risk
# ====================================================================
def test_sc_010_warn(scalability_rules):
    rule = scalability_rules["SCALABILITY-010"]
    contract = _make_contract(expected_gb=300.0, peak_gb=600.0)  # >= 500 GB
    cluster = {"num_workers": 2}  # < 4 workers
    finding = rule.evaluate({"contract": contract, "cluster_config": cluster})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_010_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-010"]
    contract = _make_contract(expected_gb=300.0, peak_gb=600.0)
    cluster = {"num_workers": 8}  # >= 4 workers
    finding = rule.evaluate({"contract": contract, "cluster_config": cluster})
    assert finding is None


def test_sc_010_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-010"]
    finding = rule.evaluate({})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-011: Autoscaling Boundary Risk
# ====================================================================
def test_sc_011_warn_fixed_cluster(scalability_rules):
    rule = scalability_rules["SCALABILITY-011"]
    contract = _make_contract(expected_gb=100.0, peak_gb=400.0)  # 4.0x surge >= 3.0x
    cluster = {"num_workers": 4}  # fixed cluster
    finding = rule.evaluate({"contract": contract, "cluster_config": cluster})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_011_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-011"]
    contract = _make_contract(expected_gb=100.0, peak_gb=400.0)
    cluster = {"autoscale": {"min_workers": 2, "max_workers": 16}}  # 8x expansion
    finding = rule.evaluate({"contract": contract, "cluster_config": cluster})
    assert finding is None


def test_sc_011_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-011"]
    finding = rule.evaluate({})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-012: SLA Scalability Risk
# ====================================================================
def test_sc_012_warn(scalability_rules):
    rule = scalability_rules["SCALABILITY-012"]
    contract = _make_contract(expected_gb=100.0, peak_gb=300.0, sla_runtime_min=60.0)  # 3x surge
    # Baseline runtime 25 min -> projected runtime is 75 min > 60 min SLA
    run = RuntimeRun(run_id="r1", duration_seconds=1500.0)
    finding = rule.evaluate({"contract": contract, "runtime_run": run})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_012_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-012"]
    contract = _make_contract(expected_gb=100.0, peak_gb=200.0, sla_runtime_min=60.0)  # 2x surge
    # Baseline runtime 10 min -> projected runtime is 20 min <= 60 min SLA
    run = RuntimeRun(run_id="r1", duration_seconds=600.0)
    finding = rule.evaluate({"contract": contract, "runtime_run": run})
    assert finding is None


def test_sc_012_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-012"]
    finding = rule.evaluate({})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-013: Runtime Regression at Scale
# ====================================================================
def test_sc_013_warn_superlinear(scalability_rules):
    rule = scalability_rules["SCALABILITY-013"]
    runs = [
        ScalabilityObservation(run_id="r1", volume_gb=100.0, duration_minutes=10.0),
        ScalabilityObservation(run_id="r2", volume_gb=200.0, duration_minutes=35.0),
    ]
    finding = rule.evaluate({"historical_runs": runs})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_013_negative_linear(scalability_rules):
    rule = scalability_rules["SCALABILITY-013"]
    runs = [
        ScalabilityObservation(run_id="r1", volume_gb=100.0, duration_minutes=10.0),
        ScalabilityObservation(run_id="r2", volume_gb=200.0, duration_minutes=20.0),
    ]
    finding = rule.evaluate({"historical_runs": runs})
    assert finding is None


def test_sc_013_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-013"]
    finding = rule.evaluate({"historical_runs": []})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN


# ====================================================================
# SCALABILITY-014: Reliability Degradation at Scale
# ====================================================================
def test_sc_014_warn_failure_escalation(scalability_rules):
    rule = scalability_rules["SCALABILITY-014"]
    runs = [
        ScalabilityObservation(
            run_id="r1",
            volume_gb=100.0,
            duration_minutes=10.0,
            task_count=100,
            failed_tasks=0,
        ),
        ScalabilityObservation(
            run_id="r2",
            volume_gb=300.0,
            duration_minutes=30.0,
            task_count=100,
            failed_tasks=10,
        ),
    ]
    finding = rule.evaluate({"historical_runs": runs})
    assert finding is not None
    assert finding.status == CheckpointStatus.WARN


def test_sc_014_negative(scalability_rules):
    rule = scalability_rules["SCALABILITY-014"]
    runs = [
        ScalabilityObservation(
            run_id="r1",
            volume_gb=100.0,
            duration_minutes=10.0,
            task_count=100,
            failed_tasks=0,
        ),
        ScalabilityObservation(
            run_id="r2",
            volume_gb=200.0,
            duration_minutes=20.0,
            task_count=200,
            failed_tasks=0,
        ),
    ]
    finding = rule.evaluate({"historical_runs": runs})
    assert finding is None


def test_sc_014_unknown(scalability_rules):
    rule = scalability_rules["SCALABILITY-014"]
    finding = rule.evaluate({"historical_runs": []})
    assert finding is not None
    assert finding.status == CheckpointStatus.UNKNOWN
