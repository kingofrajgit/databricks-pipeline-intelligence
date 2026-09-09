"""Unit tests for CP-FINAL Production Readiness Assessment.

Covers all 24 required test cases verifying deterministic status resolution,
strict three-dimensional decoupling (Score vs Coverage vs Readiness),
evidence provenance preservation, cross-domain risk synthesis, configuration
consistency, and prioritized remediation actions.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dpif.models import (
    AnalysisMethod,
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    EvidenceRecord,
    Finding,
    PipelineContract,
    ReliabilityRules,
    ScalabilityRules,
    ScheduleRules,
    Severity,
    SLARules,
    Source,
    SourceFormat,
    SourceType,
    Target,
)
from dpif.readiness.coverage import compute_comprehensive_coverage
from dpif.readiness.engine import evaluate_production_readiness
from dpif.readiness.models import (
    ProductionReadinessStatus,
    ReadinessPolicy,
)
from dpif.readiness.synthesizer import (
    compare_expected_implemented_actual,
    synthesize_cross_domain_risks,
)
from dpif.runtime.models import RuntimeRun


def _dummy_contract(
    daily_gb: float = 500.0,
    peak_gb: float = 1500.0,
    sla_min: int = 60,
    cluster_raw: dict | None = None,
) -> PipelineContract:
    contract = PipelineContract(
        contract_id="test-contract",
        pipeline_name="test_pipeline",
        environment="production",
        owner="data-team",
        source=Source(
            source_id="src-1",
            type=SourceType.ADLS,
            format=SourceFormat.DELTA,
            path="dbfs:/mnt/source",
            expected_volume_gb=daily_gb,
            peak_volume_gb=peak_gb,
        ),
        target=Target(
            target_id="tgt-1",
            type="delta",
            path="dbfs:/mnt/target",
        ),
        sla=SLARules(max_runtime_minutes=sla_min),
        schedule=ScheduleRules(frequency="daily"),
        reliability=ReliabilityRules(idempotent=True, retry_count=3),
        scalability=ScalabilityRules(forecast_horizon_days=90),
        expected_daily_volume_gb=daily_gb,
        peak_daily_volume_gb=peak_gb,
    )
    if cluster_raw:
        contract._cluster_raw = cluster_raw  # type: ignore[attr-defined]
    return contract


def _dummy_profile(total_gb: float = 500.0, file_size_kb: float = 128000.0) -> DataProfile:
    return DataProfile(
        total_bytes=int(total_gb * 1024**3),
        total_gb=total_gb,
        file_count=100,
        average_file_size_kb=file_size_kb,
        median_file_size_kb=file_size_kb,
        p95_file_size_kb=file_size_kb,
        p99_file_size_kb=file_size_kb,
        min_file_size_kb=file_size_kb,
        max_file_size_kb=file_size_kb,
        record_count=10_000_000,
        partition_count=10,
        partition_distribution={},
        schema="",
        column_count=20,
        null_distribution={},
        duplicate_indicators=[],
        analysis_method=AnalysisMethod.METADATA,
    )


def _make_cp(
    cid: str,
    cat: str,
    status: CheckpointStatus,
    score: float = 1.0,
    findings: list[Finding] | None = None,
) -> Checkpoint:
    cp = Checkpoint(
        checkpoint_id=cid,
        name=f"Checkpoint {cid}",
        category=cat,
        status=status,
        severity=Severity.INFO if status == CheckpointStatus.PASS else Severity.MEDIUM,
        score=score,
    )
    if findings:
        cp.findings = findings
    return cp


def _make_finding(
    rule_id: str,
    severity: Severity,
    blocking: bool = False,
    title: str = "Issue Title",
) -> Finding:
    return Finding(
        finding_id=f"f-{rule_id}",
        rule_id=rule_id,
        name=title,
        title=title,
        description="Detailed issue description",
        category="code",
        status=(
            CheckpointStatus.FAIL
            if severity in (Severity.CRITICAL, Severity.HIGH)
            else CheckpointStatus.WARN
        ),
        severity=severity,
        pipeline_name="test_pipeline",
        timestamp=datetime.now(UTC),
        evidence=EvidenceRecord(
            rule_id=rule_id,
            status=(
                CheckpointStatus.FAIL
                if severity in (Severity.CRITICAL, Severity.HIGH)
                else CheckpointStatus.WARN
            ),
            severity=severity,
            confidence=1.0,
            evidence=["Static scan detected pattern"],
        ),
        recommendation="Fix the issue",
        confidence=1.0,
        blocking=blocking,
    )


# 1. All Evaluated Checks Pass -> PRODUCTION_READY
def test_all_evaluable_pass_meets_thresholds_production_ready():
    checkpoints = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-004": _make_cp("CP-004", "code", CheckpointStatus.PASS, 1.0),
        "CP-007": _make_cp("CP-007", "data", CheckpointStatus.PASS, 1.0),
        "CP-009": _make_cp("CP-009", "cluster", CheckpointStatus.PASS, 1.0),
        "CP-010": _make_cp("CP-010", "scalability", CheckpointStatus.PASS, 1.0),
        "CP-011": _make_cp("CP-011", "job", CheckpointStatus.PASS, 1.0),
        "CP-012": _make_cp("CP-012", "pipeline", CheckpointStatus.PASS, 1.0),
        "CP-014": _make_cp("CP-014", "reliability", CheckpointStatus.PASS, 1.0),
        "CP-020": _make_cp("CP-020", "security", CheckpointStatus.PASS, 1.0),
        "CP-021": _make_cp("CP-021", "governance", CheckpointStatus.PASS, 1.0),
        "CP-022": _make_cp("CP-022", "data-quality", CheckpointStatus.PASS, 1.0),
    }
    policy = ReadinessPolicy(minimum_quality_score=80.0, minimum_evidence_coverage=50.0)
    assessment = evaluate_production_readiness(checkpoints=checkpoints, policy=policy)
    assert assessment.status == ProductionReadinessStatus.PRODUCTION_READY
    assert assessment.quality_score >= 80.0
    assert assessment.blocking_findings == []


# 2. Non-blocking WARN present -> PRODUCTION_READY_WITH_WARNINGS
def test_non_blocking_warn_production_ready_with_warnings():
    warn_finding = Finding(
        finding_id="f-warn",
        rule_id="CODE-PYSPARK-008",
        name="Large Shuffle",
        title="Potential Large Shuffle",
        description="Non-blocking warning",
        category="code",
        status=CheckpointStatus.WARN,
        severity=Severity.MEDIUM,
        pipeline_name="test_pipeline",
        timestamp=datetime.now(UTC),
        evidence=EvidenceRecord(
            rule_id="CODE-PYSPARK-008",
            status=CheckpointStatus.WARN,
            severity=Severity.MEDIUM,
            confidence=0.7,
        ),
        recommendation="Verify partition keys",
        confidence=0.7,
        blocking=False,
    )
    checkpoints = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-004": _make_cp("CP-004", "code", CheckpointStatus.WARN, 0.8, [warn_finding]),
        "CP-007": _make_cp("CP-007", "data", CheckpointStatus.PASS, 1.0),
        "CP-009": _make_cp("CP-009", "cluster", CheckpointStatus.PASS, 1.0),
        "CP-010": _make_cp("CP-010", "scalability", CheckpointStatus.PASS, 1.0),
        "CP-011": _make_cp("CP-011", "job", CheckpointStatus.PASS, 1.0),
        "CP-012": _make_cp("CP-012", "pipeline", CheckpointStatus.PASS, 1.0),
        "CP-014": _make_cp("CP-014", "reliability", CheckpointStatus.PASS, 1.0),
        "CP-020": _make_cp("CP-020", "security", CheckpointStatus.PASS, 1.0),
    }
    policy = ReadinessPolicy(minimum_quality_score=75.0, minimum_evidence_coverage=50.0)
    assessment = evaluate_production_readiness(checkpoints=checkpoints, policy=policy)
    assert assessment.status == ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS
    assert assessment.warning_count >= 1


# 3. Blocking CRITICAL present -> NOT_PRODUCTION_READY
def test_blocking_critical_not_production_ready_even_with_high_score():
    crit_finding = _make_finding(
        "CODE-PYSPARK-001", Severity.CRITICAL, blocking=True, title="Driver OOM Collection"
    )
    checkpoints = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-004": _make_cp("CP-004", "code", CheckpointStatus.FAIL, 0.9, [crit_finding]),
        "CP-007": _make_cp("CP-007", "data", CheckpointStatus.PASS, 1.0),
        "CP-009": _make_cp("CP-009", "cluster", CheckpointStatus.PASS, 1.0),
        "CP-020": _make_cp("CP-020", "security", CheckpointStatus.PASS, 1.0),
    }
    policy = ReadinessPolicy(minimum_quality_score=60.0, minimum_evidence_coverage=40.0)
    assessment = evaluate_production_readiness(checkpoints=checkpoints, policy=policy)
    assert assessment.status == ProductionReadinessStatus.NOT_PRODUCTION_READY
    assert len(assessment.critical_findings) >= 1
    assert any("Driver OOM Collection" in r for r in assessment.decision_reasons)


# 4. Blocking HIGH present -> NOT_PRODUCTION_READY
def test_blocking_high_not_production_ready():
    high_finding = _make_finding(
        "SECURITY-001", Severity.HIGH, blocking=True, title="Hardcoded Credentials"
    )
    checkpoints = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-020": _make_cp("CP-020", "security", CheckpointStatus.FAIL, 0.8, [high_finding]),
    }
    policy = ReadinessPolicy(
        minimum_quality_score=50.0, minimum_evidence_coverage=30.0, block_on_high=True
    )
    assessment = evaluate_production_readiness(checkpoints=checkpoints, policy=policy)
    assert assessment.status == ProductionReadinessStatus.NOT_PRODUCTION_READY
    assert any("Hardcoded Credentials" in r for r in assessment.decision_reasons)


# 5. Score below minimum threshold -> NOT_PRODUCTION_READY
def test_score_below_minimum_not_production_ready():
    checkpoints = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-004": _make_cp("CP-004", "code", CheckpointStatus.WARN, 0.6),
        "CP-010": _make_cp("CP-010", "scalability", CheckpointStatus.WARN, 0.6),
    }
    policy = ReadinessPolicy(minimum_quality_score=80.0, minimum_evidence_coverage=10.0)
    assessment = evaluate_production_readiness(checkpoints=checkpoints, policy=policy)
    assert assessment.status == ProductionReadinessStatus.NOT_PRODUCTION_READY
    assert any("below the minimum threshold" in r for r in assessment.decision_reasons)


# 6. Missing runtime evidence when required -> INSUFFICIENT_EVIDENCE
def test_missing_runtime_evidence_when_required_insufficient_evidence():
    checkpoints = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-008": _make_cp("CP-008", "performance", CheckpointStatus.UNKNOWN, 0.0),
    }
    policy = ReadinessPolicy(require_runtime_evidence=True)
    assessment = evaluate_production_readiness(
        checkpoints=checkpoints, policy=policy, runtime_data=None
    )
    assert assessment.status == ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
    assert any("runtime execution telemetry" in r.lower() for r in assessment.decision_reasons)


# 7. Missing scalability evidence when required -> INSUFFICIENT_EVIDENCE
def test_missing_scalability_evidence_when_required_insufficient_evidence():
    checkpoints = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-010": _make_cp("CP-010", "scalability", CheckpointStatus.UNKNOWN, 0.0),
    }
    policy = ReadinessPolicy(require_scalability_evidence=True)
    assessment = evaluate_production_readiness(checkpoints=checkpoints, policy=policy)
    assert assessment.status == ProductionReadinessStatus.INSUFFICIENT_EVIDENCE


# 8. Overall coverage below threshold -> INSUFFICIENT_EVIDENCE
def test_coverage_below_minimum_insufficient_evidence():
    checkpoints = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-008": _make_cp("CP-008", "performance", CheckpointStatus.UNKNOWN, 0.0),
        "CP-010": _make_cp("CP-010", "scalability", CheckpointStatus.UNKNOWN, 0.0),
        "CP-019": _make_cp("CP-019", "cost", CheckpointStatus.UNKNOWN, 0.0),
        "CP-023": _make_cp("CP-023", "sla", CheckpointStatus.UNKNOWN, 0.0),
    }
    policy = ReadinessPolicy(minimum_evidence_coverage=85.0)
    assessment = evaluate_production_readiness(checkpoints=checkpoints, policy=policy)
    assert assessment.status == ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
    assert any("evidence coverage" in r.lower() for r in assessment.decision_reasons)


# 9. Decoupling: Score vs Blocking Finding
def test_decoupling_score_vs_blocking_finding():
    crit = _make_finding(
        "CODE-PYSPARK-001", Severity.CRITICAL, blocking=True, title="Driver Out-of-Memory"
    )
    cps = {
        f"CP-{i:03d}": _make_cp(f"CP-{i:03d}", "cat", CheckpointStatus.PASS, 1.0)
        for i in range(1, 15)
    }
    cps["CP-004"].findings = [crit]
    cps["CP-004"].status = CheckpointStatus.FAIL

    assessment = evaluate_production_readiness(checkpoints=cps)
    # Quality score can be high, but status is blocked by CRITICAL finding!
    assert assessment.quality_score > 80.0
    assert assessment.status == ProductionReadinessStatus.NOT_PRODUCTION_READY


# 10. Decoupling: Score vs Evidence Coverage
def test_decoupling_score_vs_evidence_coverage():
    # 5 passing checks and 1 unknown check -> quality score 83.3% >= 80%, but coverage 83.3% < 90%
    cps = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-004": _make_cp("CP-004", "code", CheckpointStatus.PASS, 1.0),
        "CP-007": _make_cp("CP-007", "data", CheckpointStatus.PASS, 1.0),
        "CP-009": _make_cp("CP-009", "cluster", CheckpointStatus.PASS, 1.0),
        "CP-011": _make_cp("CP-011", "job", CheckpointStatus.PASS, 1.0),
        "CP-008": _make_cp("CP-008", "performance", CheckpointStatus.UNKNOWN, 0.0),
    }
    policy = ReadinessPolicy(minimum_quality_score=80.0, minimum_evidence_coverage=90.0)
    assessment = evaluate_production_readiness(checkpoints=cps, policy=policy)
    assert assessment.quality_score >= 80.0
    assert assessment.evidence_coverage < 90.0
    assert assessment.status == ProductionReadinessStatus.INSUFFICIENT_EVIDENCE


# 11. Decoupling: Coverage vs Readiness on Failure
def test_decoupling_coverage_vs_readiness_on_failure():
    # 100% evidence coverage achieved, but with a critical failure
    crit = _make_finding("SEC-001", Severity.CRITICAL, blocking=True, title="Plaintext Secret")
    cps = {
        f"CP-{i:03d}": _make_cp(f"CP-{i:03d}", "cat", CheckpointStatus.PASS, 1.0)
        for i in range(1, 24)
    }
    cps["CP-020"] = _make_cp("CP-020", "security", CheckpointStatus.FAIL, 0.0, [crit])

    assessment = evaluate_production_readiness(checkpoints=cps)
    assert assessment.evidence_coverage == 100.0
    assert assessment.status == ProductionReadinessStatus.NOT_PRODUCTION_READY


# 12. Decoupling: SLA Projected vs Observed Actual
def test_decoupling_sla_projected_vs_observed_actual():
    # CP-023 is PASS (observed 20m vs 60m SLA), but CP-010 warns projected SLA violation
    cp023 = _make_cp("CP-023", "sla", CheckpointStatus.PASS, 1.0)
    proj_finding = Finding(
        finding_id="f-proj-sla",
        rule_id="SCALABILITY-012",
        name="SLA Scalability Risk",
        title="Projected SLA Violation",
        description="At 10x volume, duration projected to exceed SLA.",
        category="scalability",
        status=CheckpointStatus.WARN,
        severity=Severity.MEDIUM,
        pipeline_name="test_pipeline",
        timestamp=datetime.now(UTC),
        evidence=EvidenceRecord(
            rule_id="SCALABILITY-012",
            status=CheckpointStatus.WARN,
            severity=Severity.MEDIUM,
            confidence=0.8,
            evidence=["Projected linear model indicates 75m duration > 60m SLA"],
        ),
        recommendation="Scale cluster prior to 10x volume expansion",
        confidence=0.8,
        blocking=False,
    )
    cp010 = _make_cp("CP-010", "scalability", CheckpointStatus.WARN, 0.8, [proj_finding])
    cps = {"CP-023": cp023, "CP-010": cp010}
    assessment = evaluate_production_readiness(checkpoints=cps)
    # SLA observed check passed; projected violation is captured as warning
    assert cp023.status == CheckpointStatus.PASS
    assert any(f.rule_id == "SCALABILITY-012" for f in assessment.warning_findings)


# 13. Static vs Runtime Confirmation Cross-Domain Risk
def test_static_runtime_confirmation_cross_domain_risk():
    code_finding = _make_finding(
        "SCALABILITY-007", Severity.MEDIUM, blocking=False, title="Potential Large Shuffle"
    )
    cps = {"CP-004": _make_cp("CP-004", "code", CheckpointStatus.WARN, 0.8, [code_finding])}
    runtime = RuntimeRun(
        run_id="run-1",
        job_id="job-1",
        run_name="test-run",
        cluster_id="c-1",
        spark_version="14.3.x-scala2.12",
        start_time=datetime.now(UTC),
        duration_seconds=120.0,
        status="SUCCESS",
        total_input_bytes=100 * 1024**3,
        total_shuffle_read_bytes=80 * 1024**3,
        total_shuffle_write_bytes=80 * 1024**3,
        spill_to_disk_bytes=15 * 1024**3,
    )
    risks = synthesize_cross_domain_risks(cps, None, None, None, runtime)
    assert any(r.risk_id == "XDOM-003" for r in risks)


# 14. Fixture vs Live Evidence Labeling
def test_fixture_vs_live_evidence_labeling():
    cp007 = _make_cp("CP-007", "data", CheckpointStatus.PASS, 1.0)
    cp007.evidence = EvidenceRecord(
        rule_id="CP-007",
        status=CheckpointStatus.PASS,
        severity=Severity.INFO,
        confidence=0.8,
        evidence=["fixture metadata"],
    )
    cps = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-007": cp007,
    }
    cov = compute_comprehensive_coverage(cps)
    data_cov = cov.domain_coverages.get("Data")
    assert data_cov is not None
    assert "FIXTURE" in data_cov.provenance_breakdown


# 15. Strict UNKNOWN Semantics Preservation
def test_strict_unknown_semantics_preservation():
    cps = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.PASS, 1.0),
        "CP-008": _make_cp("CP-008", "performance", CheckpointStatus.UNKNOWN, 0.0),
    }
    cps["CP-008"].evidence = EvidenceRecord(
        rule_id="CP-008",
        status=CheckpointStatus.UNKNOWN,
        severity=Severity.INFO,
        confidence=0.0,
        evidence=["No runtime metrics available"],
    )
    assessment = evaluate_production_readiness(checkpoints=cps)
    assert assessment.unknown_checkpoint_count >= 1
    assert cps["CP-008"].status == CheckpointStatus.UNKNOWN
    assert cps["CP-008"].evidence.confidence == 0.0


# 16. Configuration Drift Detection
def test_configuration_drift_detection():
    contract = _dummy_contract(cluster_raw={"spark_version": "14.3.x-scala2.12"})
    actual_env = {"spark_version": "13.3.x-scala2.12"}
    comparisons = compare_expected_implemented_actual(
        contract=contract,
        cluster_config={"spark_version": "14.3.x-scala2.12"},
        job_config={},
        profile=_dummy_profile(),
        runtime_data=None,
        actual_environment=actual_env,
    )
    dbr_comp = next((c for c in comparisons if c.parameter == "Spark/DBR Version"), None)
    assert dbr_comp is not None
    assert dbr_comp.is_drift is True
    assert dbr_comp.drift_severity == "BLOCKING"


# 17. Driver Memory Synthesized Risk
def test_driver_memory_synthesized_risk():
    collect_finding = _make_finding(
        "CODE-PYSPARK-006", Severity.HIGH, blocking=False, title="Driver Collection"
    )
    cps = {"CP-004": _make_cp("CP-004", "code", CheckpointStatus.WARN, 0.8, [collect_finding])}
    contract = _dummy_contract(daily_gb=500.0)
    profile = _dummy_profile(500.0)
    cluster = {"driver_node_type_id": "i3.large"}
    risks = synthesize_cross_domain_risks(cps, contract, profile, cluster, None)
    assert any(r.risk_id == "XDOM-001" for r in risks)


# 18. Cartesian Product Synthesized Risk
def test_cartesian_product_synthesized_risk():
    cartesian_finding = _make_finding(
        "CODE-PYSPARK-005", Severity.HIGH, blocking=False, title="Cartesian Join"
    )
    cps = {"CP-004": _make_cp("CP-004", "code", CheckpointStatus.WARN, 0.8, [cartesian_finding])}
    contract = _dummy_contract(daily_gb=100.0)
    profile = _dummy_profile(100.0)
    risks = synthesize_cross_domain_risks(cps, contract, profile, None, None)
    assert any(r.risk_id == "XDOM-002" for r in risks)


# 19. Shuffle and Spill Synthesized Risk
def test_shuffle_and_spill_synthesized_risk():
    shuffle_finding = _make_finding(
        "RUNTIME-PERF-002", Severity.MEDIUM, blocking=False, title="Large Shuffle"
    )
    cps = {"CP-004": _make_cp("CP-004", "code", CheckpointStatus.WARN, 0.8, [shuffle_finding])}
    runtime = RuntimeRun(
        run_id="run-spill",
        job_id="job-1",
        run_name="spill-run",
        cluster_id="c-1",
        spark_version="14.3.x-scala2.12",
        start_time=datetime.now(UTC),
        duration_seconds=300.0,
        status="SUCCESS",
        total_input_bytes=100 * 1024**3,
        total_shuffle_read_bytes=80 * 1024**3,
        total_shuffle_write_bytes=80 * 1024**3,
        spill_to_disk_bytes=25 * 1024**3,
    )
    risks = synthesize_cross_domain_risks(cps, None, None, None, runtime)
    assert any(r.risk_id == "XDOM-003" for r in risks)


# 20. Small Files and Streaming Synthesized Risk
def test_small_files_and_streaming_synthesized_risk():
    small_file_finding = _make_finding(
        "DATA-001", Severity.HIGH, blocking=False, title="Severe Small Files"
    )
    cps = {"CP-007": _make_cp("CP-007", "data", CheckpointStatus.WARN, 0.8, [small_file_finding])}
    contract = _dummy_contract(daily_gb=50.0)
    contract.processing = "streaming"
    profile = _dummy_profile(50.0, file_size_kb=32.0)
    risks = synthesize_cross_domain_risks(cps, contract, profile, None, None)
    assert any(r.risk_id == "XDOM-005" for r in risks)


# 21. Decision Reasons Contain Explanations
def test_decision_reasons_contain_actionable_explanations():
    crit = _make_finding("CODE-PYSPARK-001", Severity.CRITICAL, blocking=True, title="Driver OOM")
    cps = {"CP-004": _make_cp("CP-004", "code", CheckpointStatus.FAIL, 0.0, [crit])}
    assessment = evaluate_production_readiness(checkpoints=cps)
    assert len(assessment.decision_reasons) > 0
    assert any("Driver OOM" in r or "CODE-PYSPARK-001" in r for r in assessment.decision_reasons)


# 22. Prioritized Actions Ordering (P0 -> P1 -> P2 -> P3)
def test_prioritized_actions_order_p0_to_p3():
    crit = _make_finding("F-CRIT", Severity.CRITICAL, blocking=True, title="Critical Finding")
    high = _make_finding("F-HIGH", Severity.HIGH, blocking=False, title="High Finding")
    warn = _make_finding("F-WARN", Severity.MEDIUM, blocking=False, title="Warn Finding")
    cps = {
        "CP-001": _make_cp("CP-001", "source", CheckpointStatus.FAIL, 0.5, [crit, high, warn])
    }
    assessment = evaluate_production_readiness(checkpoints=cps)
    priorities = [a.priority for a in assessment.required_actions]
    # Check that P0 precedes P1, which precedes P2
    p0_idx = priorities.index("P0") if "P0" in priorities else -1
    p1_idx = priorities.index("P1") if "P1" in priorities else -1
    p2_idx = priorities.index("P2") if "P2" in priorities else -1
    assert p0_idx != -1
    if p1_idx != -1:
        assert p0_idx < p1_idx
    if p2_idx != -1 and p1_idx != -1:
        assert p1_idx < p2_idx


# 23. Policy Override Behavior (block_on_high)
def test_policy_override_block_on_high():
    high_finding = _make_finding(
        "F-HIGH", Severity.HIGH, blocking=False, title="High Vulnerability"
    )
    cps = {"CP-004": _make_cp("CP-004", "code", CheckpointStatus.WARN, 0.85, [high_finding])}
    # By default block_on_high is False:
    policy_default = ReadinessPolicy(
        block_on_high=False, minimum_quality_score=50.0, minimum_evidence_coverage=20.0
    )
    res_default = evaluate_production_readiness(checkpoints=cps, policy=policy_default)
    assert res_default.status == ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS

    # With block_on_high=True:
    policy_strict = ReadinessPolicy(
        block_on_high=True, minimum_quality_score=50.0, minimum_evidence_coverage=20.0
    )
    res_strict = evaluate_production_readiness(checkpoints=cps, policy=policy_strict)
    assert res_strict.status == ProductionReadinessStatus.NOT_PRODUCTION_READY


# 24. Edge Case: Empty Checkpoint Set
def test_empty_checkpoint_set_handles_gracefully():
    assessment = evaluate_production_readiness(checkpoints={})
    assert assessment.status == ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
    assert assessment.applicable_checkpoint_count == 0
    assert assessment.quality_score == 0.0
