"""Unit tests for M5G: Three-Layer Alignment & Configuration Drift Forensics.

Verifies deterministic alignment and drift detection across:
- Layer 1: EXPECTED (Contract)
- Layer 2: IMPLEMENTED (Code, Job, Cluster, M5E, M5F)
- Layer 3: ACTUAL (Workspace Cluster, Runtime Run Telemetry, Historical Runs)
"""

from __future__ import annotations

from dpif.analyzers.alignment import ThreeLayerAlignmentAnalyzer
from dpif.code.parser import analyze_source
from dpif.contract.loader import contract_cluster_job
from dpif.models import (
    CheckpointStatus,
    DataProfile,
    PipelineContract,
    ReliabilityRules,
    Severity,
    SLARules,
    Source,
    SourceFormat,
    SourceType,
    Target,
)
from dpif.models.alignment import (
    AlignmentDimension,
    DriftSeverity,
)
from dpif.models.implementation import EvidenceProvenanceKind
from dpif.models.rerun import (
    DuplicateDataRiskAnalysis,
    IdempotencyAssessment,
    RerunAnalysisResult,
)


def _make_dummy_contract(
    name: str = "test_pipeline",
    dbr: str = "15.4.x-scala2.12",
    num_workers: int = 2,
    target_format: str = "delta",
    proc_type: str = "incremental",
    max_runtime_minutes: float = 30.0,
    expected_gb: float = 10.0,
    peak_gb: float = 25.0,
    idempotent: bool = True,
    retry_count: int = 2,
    partitioning: list[str] | None = None,
) -> PipelineContract:
    contract = PipelineContract(
        contract_id="contract_001",
        pipeline_name=name,
        environment="development",
        source=Source(
            source_id="src_01",
            type=SourceType.ADLS,
            format=SourceFormat.PARQUET,
            path="abfss://landing@acct.dfs.core.windows.net/data/",
            expected_volume_gb=expected_gb,
            peak_volume_gb=peak_gb,
            partitioning=partitioning or [],
        ),
        target=Target(
            target_id="tgt_01",
            type="delta",
            format=target_format,
            catalog="dev",
            schema="marts",
            path="abfss://curated@acct.dfs.core.windows.net/target/",
        ),
        processing=proc_type,
        sla=SLARules(max_runtime_minutes=max_runtime_minutes),
        reliability=ReliabilityRules(retry_count=retry_count, idempotent=idempotent),
        cluster={"num_workers": num_workers, "spark_version": dbr, "node_type": "Standard_D4ds_v5"},
        job={"max_retries": retry_count, "timeout_seconds": int(max_runtime_minutes * 60)},
    )
    contract._cluster_raw = {
        "num_workers": num_workers,
        "spark_version": dbr,
        "node_type": "Standard_D4ds_v5",
    }
    return contract


def _analyze(code_text: str, context: dict | None = None) -> ThreeLayerAlignmentAnalyzer:
    code_analysis = analyze_source(code_text, filename="pipeline.py") if code_text else None
    if code_analysis and code_text:
        code_analysis._raw_source = code_text
    ctx = dict(context or {})
    ctx["code_snippet"] = code_text
    analyzer = ThreeLayerAlignmentAnalyzer(code_analysis, context=ctx)
    return analyzer


# -----------------------------------------------------------------------------
# Scenario A: Perfect alignment across all 3 layers -> PASS, DriftSeverity.NONE
# -----------------------------------------------------------------------------
def test_scenario_a_perfect_alignment():
    contract = _make_dummy_contract()
    cluster_cfg, job_cfg = contract_cluster_job(contract)

    code = """
df = spark.read.table("source_events").filter("event_time > current_timestamp() - interval 1 day")
df.write.format("delta").mode("append").saveAsTable("dev.marts.target")
"""
    profile = DataProfile(total_gb=10.0)

    class MockRun:
        duration_seconds = 1200.0  # 20m < 30m
        total_input_gb = 10.0

    ctx = {
        "pipeline_contract": contract,
        "cluster_config": cluster_cfg,
        "job_config": job_cfg,
        "workspace_cluster": {"spark_version": "15.4.x-scala2.12", "num_workers": 2},
        "runtime_run": MockRun(),
        "data_profile": profile,
    }

    res = _analyze(code, ctx).analyze()
    assert res.overall_status in (CheckpointStatus.PASS, CheckpointStatus.UNKNOWN)
    assert not res.has_blocking_drift
    assert res.drift_severity in (DriftSeverity.NONE, DriftSeverity.WARN)


# -----------------------------------------------------------------------------
# Scenario B: DBR Version Divergence (Contract 15.4 vs Job 14.3) -> WARN
# -----------------------------------------------------------------------------
def test_scenario_b_dbr_version_divergence():
    contract = _make_dummy_contract(dbr="15.4.x-scala2.12")
    cluster_cfg = {"spark_version": "14.3.x-scala2.12", "num_workers": 2}

    ctx = {
        "pipeline_contract": contract,
        "cluster_config": cluster_cfg,
    }

    res = _analyze("", ctx).analyze()
    dim = res.dimensions[AlignmentDimension.COMPUTE_RUNTIME.value]
    assert dim.status == CheckpointStatus.WARN
    assert dim.drift_severity == DriftSeverity.WARN
    assert any(f.rule_id == "ALIGN-COMP-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario B2: Workspace Cluster DBR Mismatch -> BLOCKING
# -----------------------------------------------------------------------------
def test_scenario_b2_workspace_dbr_blocking_divergence():
    contract = _make_dummy_contract(dbr="15.4.x-scala2.12")
    cluster_cfg = {"spark_version": "15.4.x-scala2.12", "num_workers": 2}
    workspace_cluster = {"spark_version": "13.3.x-scala2.12", "num_workers": 2}

    ctx = {
        "pipeline_contract": contract,
        "cluster_config": cluster_cfg,
        "workspace_cluster": workspace_cluster,
    }

    res = _analyze("", ctx).analyze()
    dim = res.dimensions[AlignmentDimension.COMPUTE_RUNTIME.value]
    assert dim.status == CheckpointStatus.FAIL
    assert dim.drift_severity == DriftSeverity.BLOCKING
    assert any(f.rule_id == "ALIGN-COMP-002" and f.blocking for f in dim.findings)
    assert res.has_blocking_drift


# -----------------------------------------------------------------------------
# Scenario C: Cluster Worker Sizing & Autoscale Divergence -> WARN
# -----------------------------------------------------------------------------
def test_scenario_c_cluster_worker_autoscale_drift():
    contract = _make_dummy_contract(num_workers=2)
    cluster_cfg = {
        "spark_version": "15.4.x-scala2.12",
        "autoscale": {"min_workers": 2, "max_workers": 8},
    }

    ctx = {
        "pipeline_contract": contract,
        "cluster_config": cluster_cfg,
    }

    res = _analyze("", ctx).analyze()
    dim = res.dimensions[AlignmentDimension.CLUSTER_SIZING_SCALING.value]
    assert dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "ALIGN-CLUS-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario D: Processing Strategy Contradiction (Incremental vs Full Scan) -> BLOCKING
# -----------------------------------------------------------------------------
def test_scenario_d_incremental_contract_full_scan_code():
    contract = _make_dummy_contract(proc_type="incremental")
    # Code performs unconstrained full table scan without date/time filtering, watermark, or checkpoint
    code = """
df = spark.read.table("all_historical_records")
df.write.format("delta").saveAsTable("target")
"""
    ctx = {"pipeline_contract": contract}
    res = _analyze(code, ctx).analyze()
    dim = res.dimensions[AlignmentDimension.PROCESSING_STRATEGY.value]
    assert dim.status == CheckpointStatus.FAIL
    assert dim.drift_severity == DriftSeverity.BLOCKING
    assert any(f.rule_id == "ALIGN-PROC-001" and f.blocking for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario E: Target Storage Format Contradiction (Contract Delta vs Code Parquet) -> BLOCKING
# -----------------------------------------------------------------------------
def test_scenario_e_target_format_contradiction():
    contract = _make_dummy_contract(target_format="delta")
    code = """
df = spark.read.table("events")
df.write.format("parquet").saveAsTable("target")
"""
    ctx = {"pipeline_contract": contract}
    res = _analyze(code, ctx).analyze()
    dim = res.dimensions[AlignmentDimension.TARGET_STORAGE_FORMAT.value]
    assert dim.status == CheckpointStatus.FAIL
    assert dim.drift_severity == DriftSeverity.BLOCKING
    assert any(f.rule_id == "ALIGN-TGT-001" and f.blocking for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario F: Job Workflow Paused Schedule Divergence -> WARN
# -----------------------------------------------------------------------------
def test_scenario_f_job_paused_schedule():
    contract = _make_dummy_contract()
    contract.schedule.frequency = "daily"
    contract.schedule.time = "02:00"

    job_cfg = {
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",
            "pause_status": "PAUSED",
        }
    }

    ctx = {"pipeline_contract": contract, "job_config": job_cfg}
    res = _analyze("", ctx).analyze()
    dim = res.dimensions[AlignmentDimension.JOB_WORKFLOW_CADENCE.value]
    assert dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "ALIGN-JOB-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario G: SLA Runtime Breach -> BLOCKING
# -----------------------------------------------------------------------------
def test_scenario_g_actual_runtime_exceeds_sla():
    contract = _make_dummy_contract(max_runtime_minutes=30.0)

    class OverrunRun:
        duration_seconds = 2700.0  # 45 minutes > 30m

    ctx = {"pipeline_contract": contract, "runtime_run": OverrunRun()}
    res = _analyze("", ctx).analyze()
    dim = res.dimensions[AlignmentDimension.SLA_EXECUTION_LIMITS.value]
    assert dim.status == CheckpointStatus.FAIL
    assert dim.drift_severity == DriftSeverity.BLOCKING
    assert any(f.rule_id == "ALIGN-SLA-001" and f.blocking for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario H: Missing Actual Runtime Evidence Preserves UNKNOWN
# -----------------------------------------------------------------------------
def test_scenario_h_missing_actual_evidence_unknown():
    contract = _make_dummy_contract()
    ctx = {"pipeline_contract": contract}  # no runtime_run, no workspace_cluster
    res = _analyze("", ctx).analyze()
    # Actual layer is explicitly unobserved; SLA dimension should not fabricate breach
    dim = res.dimensions[AlignmentDimension.SLA_EXECUTION_LIMITS.value]
    assert "UNKNOWN" in dim.actual_summary
    assert not any(f.rule_id == "ALIGN-SLA-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario I: Partitioning Layout Divergence -> WARN
# -----------------------------------------------------------------------------
def test_scenario_i_partitioning_divergence():
    contract = _make_dummy_contract(partitioning=["event_date", "region"])
    code = """
df = spark.read.table("events")
df.write.partitionBy("country").saveAsTable("target")
"""
    ctx = {"pipeline_contract": contract}
    res = _analyze(code, ctx).analyze()
    dim = res.dimensions[AlignmentDimension.PARTITIONING_LAYOUT.value]
    assert dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "ALIGN-PART-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario J: Reliability Contradiction (Contract Idempotent vs M5F Duplicate Risk) -> BLOCKING
# -----------------------------------------------------------------------------
def test_scenario_j_idempotency_contract_duplicate_risk_mismatch():
    contract = _make_dummy_contract(idempotent=True)

    # Simulated M5F result detecting duplicate-data risk
    fake_rerun_res = RerunAnalysisResult(
        pipeline_name="test_pipeline",
        overall_status=CheckpointStatus.WARN,
        idempotency=IdempotencyAssessment(
            overall_status=CheckpointStatus.UNKNOWN,
            is_idempotent=False,
        ),
        duplicate_risk=DuplicateDataRiskAnalysis(
            status=CheckpointStatus.WARN,
            risk_level=Severity.HIGH,
            summary="Append write without deduplication",
        ),
    )

    ctx = {
        "pipeline_contract": contract,
        "rerun_analysis": fake_rerun_res,
    }

    res = _analyze("", ctx).analyze()
    dim = res.dimensions[AlignmentDimension.RELIABILITY_RETRY_POLICY.value]
    assert dim.status == CheckpointStatus.FAIL
    assert dim.drift_severity == DriftSeverity.BLOCKING
    assert any(f.rule_id == "ALIGN-REL-001" and f.blocking for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario K: Workload Volume Ingestion Drift Far Above Peak -> WARN
# -----------------------------------------------------------------------------
def test_scenario_k_workload_volume_surge_drift():
    contract = _make_dummy_contract(expected_gb=10.0, peak_gb=25.0)

    class LargeRun:
        total_input_gb = 100.0  # 100 GB > 1.5 * 25 GB (37.5 GB)

    ctx = {"pipeline_contract": contract, "runtime_run": LargeRun()}
    res = _analyze("", ctx).analyze()
    dim = res.dimensions[AlignmentDimension.WORKLOAD_VOLUME_BOUNDS.value]
    assert dim.status == CheckpointStatus.WARN
    assert any(f.rule_id == "ALIGN-VOL-001" for f in dim.findings)


# -----------------------------------------------------------------------------
# Scenario L: Evidence Provenance Integrity
# -----------------------------------------------------------------------------
def test_scenario_l_evidence_provenance():
    contract = _make_dummy_contract(target_format="delta")
    code = "df.write.format('parquet').saveAsTable('t')"
    ctx = {"pipeline_contract": contract}
    res = _analyze(code, ctx).analyze()
    tgt_finding = next(f for f in res.findings if f.rule_id == "ALIGN-TGT-001")
    assert tgt_finding.provenance == EvidenceProvenanceKind.STATIC_CODE
    assert "Contract target format: delta" in tgt_finding.evidence[0]


# -----------------------------------------------------------------------------
# Scenario M: JSON Output Serialization Contract
# -----------------------------------------------------------------------------
def test_scenario_m_json_serialization():
    contract = _make_dummy_contract()
    res = _analyze("", {"pipeline_contract": contract}).analyze()
    d = res.to_dict()
    assert isinstance(d, dict)
    assert "pipeline_name" in d
    assert "overall_status" in d
    assert "drift_severity" in d
    assert "dimensions" in d
    assert len(d["dimensions"]) == 9
    for dim_dict in d["dimensions"].values():
        assert "dimension" in dim_dict
        assert "status" in dim_dict
        assert "drift_severity" in dim_dict
        assert "findings" in dim_dict
