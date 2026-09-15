"""Unit test suite for M5H: Evidence Coverage, Confidence & Decision Sufficiency.

Tests all 18 scenarios required by M5H specification:
1. Complete evidence
2. Partial evidence
3. Missing runtime evidence
4. Missing historical evidence
5. Fixture evidence
6. Conflicting evidence
7. Stale evidence
8. Static-only evidence
9. High-confidence finding
10. Medium-confidence finding
11. Low-confidence finding
12. UNKNOWN decision
13. Insufficient decision
14. Interaction with M5G (three-layer alignment)
15. Interaction with M5F (rerun / idempotency)
16. Interaction with production readiness
17. JSON serialization
18. CLI rendering
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dpif.analyzers.sufficiency import EvidenceSufficiencyAnalyzer
from dpif.code.parser import analyze_source
from dpif.contract.loader import contract_cluster_job
from dpif.models import (
    AnalysisMethod,
    Checkpoint,
    CheckpointStatus,
    CollectionMethod,
    DataProfile,
    PipelineContract,
    Severity,
    Source,
    SourceFormat,
    SourceType,
    Target,
)
from dpif.models.alignment import (
    AlignmentDimension,
    DimensionAlignmentAssessment,
    DriftSeverity,
    ThreeLayerAlignmentAssessment,
)
from dpif.models.implementation import EvidenceProvenanceKind, ImplementationForensicsResult
from dpif.models.rerun import (
    DuplicateDataRiskAnalysis,
    IdempotencyAssessment,
    RerunAnalysisResult,
)
from dpif.models.sufficiency import (
    ConfidenceLevel,
    EvidenceFreshness,
    EvidenceQuality,
)


@pytest.fixture
def base_contract() -> PipelineContract:
    return PipelineContract(
        contract_id="PIPE_M5H",
        pipeline_name="EvidenceSufficiencyTestPipe",
        environment="production",
        processing="batch",
        source=Source(
            source_id="s1",
            type=SourceType.ADLS,
            path="abfss://raw@acct.dfs.core.windows.net/data",
            format=SourceFormat.PARQUET,
            expected_volume_gb=25.0,
        ),
        target=Target(
            target_id="t1",
            type=SourceType.PARQUET,
            path="abfss://gold@acct.dfs.core.windows.net/out",
            format=SourceFormat.DELTA,
        ),
    )


@pytest.fixture
def base_profile() -> DataProfile:
    return DataProfile(
        total_bytes=26843545600,
        total_gb=25.0,
        record_count=1000000,
        file_count=50,
        column_count=10,
        partition_count=4,
        average_file_size_kb=512000.0,
        median_file_size_kb=512000.0,
        p95_file_size_kb=600000.0,
        p99_file_size_kb=700000.0,
        min_file_size_kb=100000.0,
        max_file_size_kb=800000.0,
        schema="",
        analysis_method=AnalysisMethod.METADATA,
        collection_method=CollectionMethod.SAMPLE,
    )


@pytest.fixture
def sample_runtime():
    rt = MagicMock()
    rt.duration_seconds = 600.0
    rt.task_count = 120
    rt.bytes_read = 26843545600
    rt.bytes_written = 25000000000
    rt.memory_spill_bytes = 0
    rt.dbu_consumed = 4.5
    rt.timestamp = "2026-09-15T12:00:00Z"
    rt.is_stale = False
    return rt


# =============================================================================
# 1. Complete Evidence Scenario
# =============================================================================
def test_scenario_1_complete_evidence(base_contract, base_profile, sample_runtime):
    """Scenario 1: Complete evidence across all domains yields HIGH confidence and sufficiency."""
    code = "df = spark.read.parquet('in')\ndf.write.format('delta').save('out')"
    analysis = analyze_source(code)

    cluster_config, job_config = contract_cluster_job(base_contract)
    cluster_config["node_type_id"] = "Standard_D8s_v5"
    cluster_config["num_workers"] = 4
    cluster_config["spark_version"] = "14.3.x-scala2.12"
    cluster_config["pricing_tier"] = "STANDARD"

    job_config["schedule"] = {"quartz_cron_expression": "0 0 * * * ?"}
    job_config["max_concurrent_runs"] = 1

    hist_run1 = MagicMock(duration_seconds=580.0, input_bytes=25000000000)
    hist_run2 = MagicMock(duration_seconds=610.0, input_bytes=26000000000)

    impl_assess = ImplementationForensicsResult(
        pipeline_name="EvidenceSufficiencyTestPipe",
        overall_status=CheckpointStatus.PASS,
        all_findings=[],
    )
    rerun_assess = RerunAnalysisResult(
        pipeline_name="EvidenceSufficiencyTestPipe",
        idempotency=IdempotencyAssessment(overall_status=CheckpointStatus.PASS, summary="Idempotent"),
        duplicate_risk=DuplicateDataRiskAnalysis(overall_status=CheckpointStatus.PASS, summary="Safe"),
    )
    align_assess = ThreeLayerAlignmentAssessment(
        pipeline_name="EvidenceSufficiencyTestPipe",
        overall_status=CheckpointStatus.PASS,
        drift_severity=DriftSeverity.NONE,
        total_drifts=0,
        dimensions={},
        findings=[],
    )

    ctx = {
        "contract": base_contract,
        "profile": base_profile,
        "cluster_config": cluster_config,
        "job_config": job_config,
        "runtime_run": sample_runtime,
        "historical_runs": [hist_run1, hist_run2],
        "implementation_forensics": impl_assess,
        "rerun_analysis": rerun_assess,
        "alignment_analysis": align_assess,
    }

    analyzer = EvidenceSufficiencyAnalyzer(analysis, context=ctx)
    assessment = analyzer.analyze()

    assert assessment.overall_confidence == ConfidenceLevel.HIGH
    assert assessment.overall_decision_sufficiency is True
    assert assessment.domains_sufficient == 16
    assert assessment.coverage_score >= 85.0


# =============================================================================
# 2. Partial Evidence Scenario
# =============================================================================
def test_scenario_2_partial_evidence(base_contract, base_profile):
    """Scenario 2: Missing optional runtime and cost items yields MEDIUM confidence."""
    code = "df = spark.read.parquet('in')"
    analysis = analyze_source(code)

    ctx = {
        "contract": base_contract,
        "profile": base_profile,
    }

    analyzer = EvidenceSufficiencyAnalyzer(analysis, context=ctx)
    assessment = analyzer.analyze()

    assert assessment.overall_confidence in (ConfidenceLevel.MEDIUM, ConfidenceLevel.LOW)
    assert assessment.domains_sufficient < 16
    assert assessment.domains_insufficient > 0


# =============================================================================
# 3. Missing Runtime Evidence Scenario
# =============================================================================
def test_scenario_3_missing_runtime_evidence(base_contract, base_profile):
    """Scenario 3: Missing runtime evidence leaves runtime domain INSUFFICIENT with required evidence."""
    code = "df = spark.read.parquet('in')"
    analysis = analyze_source(code)

    ctx = {
        "contract": base_contract,
        "profile": base_profile,
        "runtime_run": None,
    }

    analyzer = EvidenceSufficiencyAnalyzer(analysis, context=ctx)
    assessment = analyzer.analyze()

    rt_cov = assessment.domain_coverages["runtime"]
    assert rt_cov.decision_sufficient is False
    assert rt_cov.confidence == ConfidenceLevel.INSUFFICIENT
    assert len(rt_cov.required_evidence_for_sufficiency) > 0
    assert "Observed runtime execution duration" in rt_cov.required_evidence_for_sufficiency[0]


# =============================================================================
# 4. Missing Historical Evidence Scenario
# =============================================================================
def test_scenario_4_missing_historical_evidence(base_contract):
    """Scenario 4: 0 or 1 historical runs cannot support empirical trend forensics."""
    ctx_0 = {"contract": base_contract, "historical_runs": []}
    assessment_0 = EvidenceSufficiencyAnalyzer(context=ctx_0).analyze()
    hist_cov_0 = assessment_0.domain_coverages["historical_runs"]
    assert hist_cov_0.decision_sufficient is False
    assert hist_cov_0.confidence == ConfidenceLevel.INSUFFICIENT

    single_run = MagicMock(duration_seconds=100.0)
    ctx_1 = {"contract": base_contract, "historical_runs": [single_run]}
    assessment_1 = EvidenceSufficiencyAnalyzer(context=ctx_1).analyze()
    hist_cov_1 = assessment_1.domain_coverages["historical_runs"]
    assert hist_cov_1.decision_sufficient is False
    assert hist_cov_1.confidence == ConfidenceLevel.LOW
    assert hist_cov_1.quality == EvidenceQuality.WEAK


# =============================================================================
# 5. Fixture Evidence Scenario
# =============================================================================
def test_scenario_5_fixture_evidence(base_contract):
    """Scenario 5: Fixture metadata profile carries FIXTURE provenance, not RUNTIME."""
    fixture_profile = DataProfile(
        total_bytes=10737418240,
        total_gb=10.0,
        record_count=100000,
        file_count=20,
        column_count=5,
        partition_count=1,
        average_file_size_kb=512000.0,
        median_file_size_kb=512000.0,
        p95_file_size_kb=512000.0,
        p99_file_size_kb=512000.0,
        min_file_size_kb=512000.0,
        max_file_size_kb=512000.0,
        schema="",
        analysis_method=AnalysisMethod.METADATA,
        collection_method=CollectionMethod.FIXTURE,
    )

    ctx = {"contract": base_contract, "profile": fixture_profile}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()

    data_cov = assessment.domain_coverages["data"]
    assert EvidenceProvenanceKind.FIXTURE in data_cov.provenance
    assert EvidenceProvenanceKind.RUNTIME not in data_cov.provenance


# =============================================================================
# 6. Conflicting Evidence Scenario
# =============================================================================
def test_scenario_6_conflicting_evidence(base_contract):
    """Scenario 6: Divergent configurations between contract and code are identified."""
    # Code writes parquet while contract declares delta
    code = "df.write.format('parquet').save('out')"
    analysis = analyze_source(code)

    align_assess = ThreeLayerAlignmentAssessment(
        pipeline_name="EvidenceSufficiencyTestPipe",
        overall_status=CheckpointStatus.FAIL,
        drift_severity=DriftSeverity.BLOCKING,
        total_drifts=1,
        dimensions={
            "target_storage_format": DimensionAlignmentAssessment(
                dimension=AlignmentDimension.TARGET_STORAGE_FORMAT,
                status=CheckpointStatus.FAIL,
                drift_severity=DriftSeverity.BLOCKING,
                summary="Target storage format contradiction",
            )
        },
        findings=[],
    )

    ctx = {
        "contract": base_contract,
        "alignment_analysis": align_assess,
    }

    assessment = EvidenceSufficiencyAnalyzer(analysis, context=ctx).analyze()
    conf_dec = next(d for d in assessment.decisions if d.decision_name == "CONFIGURATION_ALIGNMENT")
    assert conf_dec.decision_status == "FAIL"
    assert conf_dec.is_sufficient is True


# =============================================================================
# 7. Stale Evidence Scenario
# =============================================================================
def test_scenario_7_stale_evidence(base_contract, sample_runtime):
    """Scenario 7: Outdated/stale telemetry is classified with STALE freshness."""
    sample_runtime.is_stale = True
    sample_runtime.status = "stale"

    ctx = {"contract": base_contract, "runtime_run": sample_runtime}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()

    rt_cov = assessment.domain_coverages["runtime"]
    assert rt_cov.freshness == EvidenceFreshness.STALE


# =============================================================================
# 8. Static-Only Evidence Scenario
# =============================================================================
def test_scenario_8_static_only_evidence(base_contract):
    """Scenario 8: Static-only evidence yields STATIC freshness across static domains."""
    code = "df = spark.read.parquet('in')"
    analysis = analyze_source(code)

    ctx = {"contract": base_contract}
    assessment = EvidenceSufficiencyAnalyzer(analysis, context=ctx).analyze()

    assert assessment.domain_coverages["code"].freshness == EvidenceFreshness.STATIC
    assert assessment.domain_coverages["pipeline"].freshness == EvidenceFreshness.STATIC
    assert assessment.domain_coverages["source"].freshness == EvidenceFreshness.STATIC


# =============================================================================
# 9. High-Confidence Finding Scenario
# =============================================================================
def test_scenario_9_high_confidence_finding(base_contract, sample_runtime):
    """Scenario 9: Direct runtime telemetry supports HIGH confidence decisions."""
    ctx = {
        "contract": base_contract,
        "runtime_run": sample_runtime,
        "checkpoints": {
            "CP-023": Checkpoint(
                checkpoint_id="CP-023",
                name="SLA",
                category="sla",
                status=CheckpointStatus.PASS,
                severity=Severity.HIGH,
            )
        },
    }
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    sla_dec = next(d for d in assessment.decisions if d.decision_name == "SLA_COMPLIANCE")

    assert sla_dec.decision_status == "PASS"
    assert sla_dec.confidence == ConfidenceLevel.HIGH
    assert sla_dec.is_sufficient is True


# =============================================================================
# 10. Medium-Confidence Finding Scenario
# =============================================================================
def test_scenario_10_medium_confidence_finding(base_contract, base_profile):
    """Scenario 10: Scalability evaluated with static volume without multi-run telemetry gives MEDIUM confidence."""
    ctx = {"contract": base_contract, "profile": base_profile}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    scal_dec = next(d for d in assessment.decisions if d.decision_name == "SCALABILITY_AT_PEAK")

    assert scal_dec.confidence == ConfidenceLevel.MEDIUM
    assert scal_dec.decision_status == "WARN"


# =============================================================================
# 11. Low-Confidence Finding Scenario
# =============================================================================
def test_scenario_11_low_confidence_finding(base_contract):
    """Scenario 11: Idempotency with UNKNOWN merge keys produces LOW confidence."""
    rerun_assess = RerunAnalysisResult(
        pipeline_name="EvidenceSufficiencyTestPipe",
        idempotency=IdempotencyAssessment(overall_status=CheckpointStatus.UNKNOWN, summary="Key safety unproven"),
        duplicate_risk=DuplicateDataRiskAnalysis(overall_status=CheckpointStatus.UNKNOWN, summary="Risk unknown"),
    )

    ctx = {"contract": base_contract, "rerun_analysis": rerun_assess}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    idm_dec = next(d for d in assessment.decisions if d.decision_name == "IDEMPOTENCY_SAFETY")

    assert idm_dec.confidence == ConfidenceLevel.LOW
    assert idm_dec.is_sufficient is False
    assert len(idm_dec.required_evidence) > 0


# =============================================================================
# 12. UNKNOWN Decision Scenario
# =============================================================================
def test_scenario_12_unknown_decision(base_contract):
    """Scenario 12: Missing runtime duration leaves SLA compliance UNKNOWN with explainable required evidence."""
    ctx = {"contract": base_contract, "runtime_run": None}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    sla_dec = next(d for d in assessment.decisions if d.decision_name == "SLA_COMPLIANCE")

    assert sla_dec.decision_status == "UNKNOWN"
    assert sla_dec.is_sufficient is False
    assert "Observed runtime duration telemetry" in sla_dec.required_evidence


# =============================================================================
# 13. Insufficient Decision Scenario
# =============================================================================
def test_scenario_13_insufficient_decision(base_contract):
    """Scenario 13: Cost decision is INSUFFICIENT and lists missing telemetry."""
    ctx = {"contract": base_contract, "runtime_run": None}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    cost_dec = next(d for d in assessment.decisions if d.decision_name == "COST_VIABILITY")

    assert cost_dec.is_sufficient is False
    assert cost_dec.decision_status == "UNKNOWN"
    assert "DBU consumption rate" in cost_dec.missing_evidence


# =============================================================================
# 14. Interaction with M5G (Three-Layer Alignment)
# =============================================================================
def test_scenario_14_interaction_with_m5g(base_contract):
    """Scenario 14: Incorporates M5G alignment forensics into evidence sufficiency."""
    align_assess = ThreeLayerAlignmentAssessment(
        pipeline_name="EvidenceSufficiencyTestPipe",
        overall_status=CheckpointStatus.PASS,
        drift_severity=DriftSeverity.NONE,
        total_drifts=0,
        dimensions={},
        findings=[],
    )

    ctx = {"contract": base_contract, "alignment_analysis": align_assess}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    align_cov = assessment.domain_coverages["three_layer_alignment"]

    assert align_cov.decision_sufficient is True
    assert "nine_dimensions_evaluated" in align_cov.evidence_available


# =============================================================================
# 15. Interaction with M5F (Rerun / Idempotency)
# =============================================================================
def test_scenario_15_interaction_with_m5f(base_contract):
    """Scenario 15: Conclusive M5F rerun assessment produces sufficient idempotency decision."""
    rerun_assess = RerunAnalysisResult(
        pipeline_name="EvidenceSufficiencyTestPipe",
        idempotency=IdempotencyAssessment(overall_status=CheckpointStatus.PASS, summary="Conclusively safe"),
        duplicate_risk=DuplicateDataRiskAnalysis(overall_status=CheckpointStatus.PASS, summary="Zero duplicate risk"),
    )

    ctx = {"contract": base_contract, "rerun_analysis": rerun_assess}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    idm_dec = next(d for d in assessment.decisions if d.decision_name == "IDEMPOTENCY_SAFETY")

    assert idm_dec.decision_status == "PASS"
    assert idm_dec.is_sufficient is True
    assert idm_dec.confidence == ConfidenceLevel.HIGH


# =============================================================================
# 16. Interaction with Production Readiness
# =============================================================================
def test_scenario_16_interaction_with_production_readiness(base_contract):
    """Scenario 16: Blocking failures definitively reject production release."""
    ctx = {
        "contract": base_contract,
        "checkpoints": {
            "CP-004": Checkpoint(
                checkpoint_id="CP-004",
                name="Code Quality",
                category="code",
                status=CheckpointStatus.FAIL,
                severity=Severity.CRITICAL,
            )
        },
    }
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    rel_dec = next(d for d in assessment.decisions if d.decision_name == "PRODUCTION_RELEASE")

    assert rel_dec.decision_status == "FAIL"
    assert rel_dec.is_sufficient is True


# =============================================================================
# 17. JSON Serialization Scenario
# =============================================================================
def test_scenario_17_json_serialization(base_contract, base_profile):
    """Scenario 17: EvidenceSufficiencyAssessment serializes completely to dictionary."""
    ctx = {"contract": base_contract, "profile": base_profile}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()
    data = assessment.to_dict()

    assert "overall_confidence" in data
    assert "overall_decision_sufficiency" in data
    assert "domain_coverages" in data
    assert len(data["domain_coverages"]) == 16
    assert "decisions" in data
    assert len(data["decisions"]) >= 5
    assert "critical_missing_evidence" in data


# =============================================================================
# 18. CLI Rendering Scenario
# =============================================================================
def test_scenario_18_cli_rendering(base_contract, base_profile):
    """Scenario 18: CLI rendering function executes cleanly without exceptions."""
    from dpif.cli import _display_evidence_sufficiency_section

    ctx = {"contract": base_contract, "profile": base_profile}
    assessment = EvidenceSufficiencyAnalyzer(context=ctx).analyze()

    # Verify rendering executes without error
    _display_evidence_sufficiency_section(assessment)
