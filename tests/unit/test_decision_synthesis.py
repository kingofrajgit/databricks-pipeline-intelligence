"""Unit tests for Phase M5I: Decision & Risk Synthesis Engine.

Tests all 20 required scenarios:
1. No risks -> PRODUCTION_READY
2. Low-risk pipeline -> PRODUCTION_READY
3. Medium-risk pipeline -> CONDITIONAL
4. High-risk pipeline -> NOT_PRODUCTION_READY
5. Critical blocker -> NOT_PRODUCTION_READY
6. Multiple findings collapse into one synthesized risk (provenance preserved)
7. Risk provenance preserved to original rule IDs
8. Blocker detection from CP-024, M5F, M5G
9. Remediation aggregation with priority P0-P3
10. Evidence insufficiency -> INSUFFICIENT_EVIDENCE
11. UNKNOWN remains UNKNOWN (anti-fabrication)
12. M5H confidence integration
13. M5H decision sufficiency integration
14. M5F idempotency risk integration
15. M5G alignment drift integration
16. CP-024 production readiness integration
17. Score cannot override blocker (score >= 80 with blocker -> NOT_PRODUCTION_READY + explanation)
18. JSON serialization roundtrip
19. CLI rendering verification
20. All five representative pipelines validation
"""

import json
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from dpif.analyzers.synthesis import DecisionRiskSynthesisAnalyzer
from dpif.cli import cli
from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    EvidenceRecord,
    Finding,
    Severity,
)
from dpif.models.sufficiency import (
    ConfidenceLevel,
    DecisionSufficiencyRecord,
    DomainEvidenceCoverage,
    EvidenceQuality,
    EvidenceSufficiencyAssessment,
)
from dpif.models.synthesis import (
    FinalDecisionStatus,
    RiskCategory,
)
from dpif.readiness.models import (
    ComprehensiveEvidenceCoverage,
    DomainCoverage,
    ProductionReadinessAssessment,
    ProductionReadinessStatus,
)


def _make_finding(
    rule_id: str,
    name: str,
    status: CheckpointStatus,
    severity: Severity,
    category: str = "test",
    blocking: bool = False,
    confidence: float = 0.9,
    description: str = "",
    recommendation: str = "",
    pipeline_name: str = "test_pipeline",
) -> Finding:
    """Helper to construct valid Finding."""
    return Finding(
        finding_id=f"f-{rule_id}",
        rule_id=rule_id,
        name=name,
        title=name,
        category=category,
        status=status,
        severity=severity,
        pipeline_name=pipeline_name,
        blocking=blocking,
        confidence=confidence,
        description=description or name,
        recommendation=recommendation or f"Remediate {rule_id}",
        evidence=EvidenceRecord(rule_id=rule_id, status=status, severity=severity),
    )


def _make_dummy_readiness(
    status: ProductionReadinessStatus = ProductionReadinessStatus.PRODUCTION_READY,
    score: float = 95.0,
    blocking_findings: list[Finding] | None = None,
    critical_findings: list[Finding] | None = None,
    decision_reasons: list[str] | None = None,
) -> ProductionReadinessAssessment:
    """Helper to construct dummy ProductionReadinessAssessment."""
    return ProductionReadinessAssessment(
        status=status,
        quality_score=score,
        evidence_coverage=85.0,
        comprehensive_coverage=ComprehensiveEvidenceCoverage(
            total_required=10,
            total_evaluated=9,
            total_unknown=1,
            total_not_applicable=0,
            coverage_percentage=90.0,
            domain_coverages={
                "code": DomainCoverage(domain="code", total_checks=5, evaluated_checks=5, coverage_percentage=100.0)
            },
        ),
        decision_reasons=decision_reasons or [],
        blocking_findings=blocking_findings or [],
        critical_findings=critical_findings or [],
    )


def _make_dummy_sufficiency(
    overall_confidence: ConfidenceLevel = ConfidenceLevel.HIGH,
    decision_sufficiency: bool = True,
    decisions: list[DecisionSufficiencyRecord] | None = None,
) -> EvidenceSufficiencyAssessment:
    """Helper to construct dummy EvidenceSufficiencyAssessment."""
    if decisions is None:
        decisions = [
            DecisionSufficiencyRecord(
                decision_name="PRODUCTION_RELEASE",
                domain="readiness",
                decision_status="PASS",
                is_sufficient=decision_sufficiency,
                confidence=overall_confidence,
            )
        ]
    return EvidenceSufficiencyAssessment(
        overall_confidence=overall_confidence,
        overall_decision_sufficiency=decision_sufficiency,
        coverage_score=85.0,
        domains_evaluated=16,
        domains_sufficient=16 if decision_sufficiency else 12,
        domain_coverages={
            "source": DomainEvidenceCoverage(
                domain="source",
                confidence=ConfidenceLevel.HIGH,
                quality=EvidenceQuality.STRONG,
                completeness_score=1.0,
                decision_sufficient=True,
                evidence_unavailable=[] if decision_sufficiency else ["runtime telemetry"],
            )
        },
        decisions=decisions,
        critical_missing_evidence=[] if decision_sufficiency else ["Runtime execution duration telemetry"],
        actionable_recommendations=["Supply runtime telemetry to enable empirical performance forensics."],
    )


def _make_clean_checkpoints() -> dict[str, Checkpoint]:
    """Helper returning clean passing checkpoints."""
    return {
        "CP-001": Checkpoint(
            checkpoint_id="CP-001",
            name="Source Validation",
            category="source",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            findings=[],
        ),
        "CP-004": Checkpoint(
            checkpoint_id="CP-004",
            name="Code Validation",
            category="code",
            status=CheckpointStatus.PASS,
            severity=Severity.INFO,
            findings=[],
        ),
    }


# ---------------------------------------------------------------------------
# Test Scenarios 1 to 5: Baseline Decision Tiers
# ---------------------------------------------------------------------------


def test_scenario_1_no_risks():
    """Scenario 1: No risks -> PRODUCTION_READY."""
    cps = _make_clean_checkpoints()
    readiness = _make_dummy_readiness(status=ProductionReadinessStatus.PRODUCTION_READY, score=100.0)
    sufficiency = _make_dummy_sufficiency()

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    assert res.final_decision == FinalDecisionStatus.PRODUCTION_READY
    assert len(res.blockers) == 0
    assert len(res.all_risks) == 0
    assert res.score_override_reason is None


def test_scenario_2_low_risk_pipeline():
    """Scenario 2: Low-risk pipeline -> PRODUCTION_READY."""
    cps = _make_clean_checkpoints()
    finding = _make_finding(
        rule_id="SRC-005",
        name="Source Minor Notice",
        status=CheckpointStatus.WARN,
        severity=Severity.LOW,
        category="source",
    )
    cps["CP-001"].findings.append(finding)
    readiness = _make_dummy_readiness(status=ProductionReadinessStatus.PRODUCTION_READY, score=92.0)
    sufficiency = _make_dummy_sufficiency()

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    assert res.final_decision == FinalDecisionStatus.PRODUCTION_READY
    assert len(res.all_risks) == 1
    assert res.all_risks[0].severity == Severity.LOW


def test_scenario_3_medium_risk_pipeline():
    """Scenario 3: Medium-risk pipeline -> CONDITIONAL."""
    cps = _make_clean_checkpoints()
    finding = _make_finding(
        rule_id="SCALABILITY-002",
        name="Peak Volume Capacity Risk",
        status=CheckpointStatus.WARN,
        severity=Severity.MEDIUM,
        category="scalability",
        confidence=0.7,
    )
    cps["CP-010"] = Checkpoint(
        checkpoint_id="CP-010",
        name="Scalability Validation",
        category="scalability",
        status=CheckpointStatus.WARN,
        severity=Severity.MEDIUM,
        findings=[finding],
    )
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS, score=85.0
    )
    sufficiency = _make_dummy_sufficiency()

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    assert res.final_decision == FinalDecisionStatus.CONDITIONAL
    assert len(res.all_risks) == 1
    assert res.all_risks[0].category == RiskCategory.SCALABILITY


def test_scenario_4_high_risk_pipeline():
    """Scenario 4: High-risk pipeline -> NOT_PRODUCTION_READY."""
    cps = _make_clean_checkpoints()
    finding = _make_finding(
        rule_id="RER-DUP-001",
        name="Unprotected Append on Retry Risk",
        status=CheckpointStatus.FAIL,
        severity=Severity.HIGH,
        category="rerun_idempotency",
        confidence=0.85,
    )
    cps["CP-016"] = Checkpoint(
        checkpoint_id="CP-016",
        name="Idempotency Validation",
        category="rerun_idempotency",
        status=CheckpointStatus.FAIL,
        severity=Severity.HIGH,
        findings=[finding],
    )
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.NOT_PRODUCTION_READY,
        score=75.0,
        decision_reasons=["High severity idempotency failure"],
    )
    sufficiency = _make_dummy_sufficiency()

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    assert res.final_decision == FinalDecisionStatus.NOT_PRODUCTION_READY
    assert any(r.severity == Severity.HIGH for r in res.all_risks)


def test_scenario_5_critical_blocker():
    """Scenario 5: Critical blocker -> NOT_PRODUCTION_READY."""
    cps = _make_clean_checkpoints()
    crit_finding = _make_finding(
        rule_id="SEC-001",
        name="Hardcoded Secret Detected",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        category="security",
        blocking=True,
        confidence=1.0,
    )
    cps["CP-020"] = Checkpoint(
        checkpoint_id="CP-020",
        name="Security Validation",
        category="security",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        findings=[crit_finding],
    )
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.NOT_PRODUCTION_READY,
        score=60.0,
        blocking_findings=[crit_finding],
        critical_findings=[crit_finding],
    )
    sufficiency = _make_dummy_sufficiency()

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    assert res.final_decision == FinalDecisionStatus.NOT_PRODUCTION_READY
    assert len(res.blockers) >= 1
    assert res.blockers[0].category == RiskCategory.SECURITY


# ---------------------------------------------------------------------------
# Test Scenarios 6 to 10: Risk Consolidation, Provenance, Remediations
# ---------------------------------------------------------------------------


def test_scenario_6_multiple_findings_collapse():
    """Scenario 6: Multiple findings collapse into one synthesized risk while preserving provenance."""
    cps = _make_clean_checkpoints()
    f1 = _make_finding(
        rule_id="RER-DUP-001",
        name="Append Without Dedup",
        status=CheckpointStatus.FAIL,
        severity=Severity.HIGH,
        category="rerun_idempotency",
    )
    cps["CP-016"] = Checkpoint(
        checkpoint_id="CP-016",
        name="Idempotency Validation",
        category="rerun_idempotency",
        status=CheckpointStatus.FAIL,
        severity=Severity.HIGH,
        findings=[f1],
    )

    # Mock M5F also flagging RER-DUP-001
    mock_rf = MagicMock()
    mock_rf.rule_id = "RER-DUP-001"
    mock_rf.title = "Duplicate Data Risk: Append"
    mock_rf.description = "Duplicate records on retry"
    mock_rf.severity = Severity.HIGH
    mock_rf.status = CheckpointStatus.FAIL
    mock_rf.evidence = ["Append write mode detected"]
    mock_rerun = MagicMock()
    mock_rerun.findings = [mock_rf]

    readiness = _make_dummy_readiness(status=ProductionReadinessStatus.NOT_PRODUCTION_READY, score=70.0)
    sufficiency = _make_dummy_sufficiency()

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        rerun_analysis=mock_rerun,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    # Should collapse into a single risk with id RISK-RER-DUP-001
    dup_risks = [r for r in res.all_risks if "RER-DUP-001" in r.risk_id]
    assert len(dup_risks) == 1
    assert "RER-DUP-001" in dup_risks[0].source_findings


def test_scenario_7_risk_provenance_preserved():
    """Scenario 7: Risk provenance preserved to original rule IDs."""
    cps = _make_clean_checkpoints()
    f = _make_finding(
        rule_id="CODE-SQL-002",
        name="Cross Join Detected",
        status=CheckpointStatus.WARN,
        severity=Severity.MEDIUM,
        category="code",
    )
    cps["CP-004"].findings.append(f)
    readiness = _make_dummy_readiness(status=ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS)

    analyzer = DecisionRiskSynthesisAnalyzer(checkpoints=cps, readiness=readiness)
    res = analyzer.analyze()
    risk = next(r for r in res.all_risks if "CODE-SQL-002" in r.risk_id)
    assert risk.source_findings == ["CODE-SQL-002"]
    assert risk.category == RiskCategory.IMPLEMENTATION


def test_scenario_8_blocker_detection():
    """Scenario 8: Blocker detection from CP-024, M5F, and M5G."""
    cps = _make_clean_checkpoints()
    crit = _make_finding(
        rule_id="SEC-002",
        name="Permissive Network Ingress",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        category="security",
        blocking=True,
    )
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.NOT_PRODUCTION_READY,
        score=72.0,
        blocking_findings=[crit],
    )

    # Mock M5G with blocking configuration drift
    mock_dim = MagicMock()
    mock_dim.drift_detected = True
    mock_dim.drift_severity = "BLOCKING"
    mock_dim.expected_summary = "15.4.x-scala2.12"
    mock_dim.implemented_summary = "14.3.x-scala2.12"
    mock_dim.actual_summary = "14.3.x-scala2.12"
    mock_dim.findings = []
    mock_align = MagicMock()
    mock_align.dimensions = {"compute_runtime": mock_dim}

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        alignment_analysis=mock_align,
    )
    res = analyzer.analyze()
    assert len(res.blockers) >= 2
    blocker_sources = [b.source for b in res.blockers]
    assert "SEC-002" in blocker_sources
    assert "ALIGN-COMPUTE_RUNTIME" in blocker_sources


def test_scenario_9_remediation_aggregation():
    """Scenario 9: Remediation aggregation prioritized P0 to P3."""
    cps = _make_clean_checkpoints()
    crit = _make_finding(
        rule_id="SEC-001",
        name="Hardcoded Secret",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        category="security",
        blocking=True,
    )
    high = _make_finding(
        rule_id="RER-DUP-001",
        name="Unprotected Append",
        status=CheckpointStatus.FAIL,
        severity=Severity.HIGH,
        category="rerun_idempotency",
    )
    med = _make_finding(
        rule_id="SCALABILITY-002",
        name="Peak Headroom Risk",
        status=CheckpointStatus.WARN,
        severity=Severity.MEDIUM,
        category="scalability",
    )
    cps["CP-001"].findings.extend([crit, high, med])
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.NOT_PRODUCTION_READY,
        score=65.0,
        blocking_findings=[crit],
    )

    analyzer = DecisionRiskSynthesisAnalyzer(checkpoints=cps, readiness=readiness)
    res = analyzer.analyze()
    priorities = [rem.priority for rem in res.remediations]
    assert "P0" in priorities
    assert "P1" in priorities
    assert "P2" in priorities


def test_scenario_10_evidence_insufficiency():
    """Scenario 10: Evidence insufficiency -> INSUFFICIENT_EVIDENCE."""
    cps = _make_clean_checkpoints()
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.INSUFFICIENT_EVIDENCE,
        score=88.0,
        decision_reasons=["Missing runtime execution telemetry"],
    )
    dec = DecisionSufficiencyRecord(
        decision_name="SLA_COMPLIANCE",
        domain="runtime",
        decision_status="UNKNOWN",
        is_sufficient=False,
        confidence=ConfidenceLevel.INSUFFICIENT,
    )
    sufficiency = _make_dummy_sufficiency(
        overall_confidence=ConfidenceLevel.INSUFFICIENT,
        decision_sufficiency=False,
        decisions=[dec],
    )

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    assert res.final_decision == FinalDecisionStatus.INSUFFICIENT_EVIDENCE
    assert not res.decision_sufficiency


# ---------------------------------------------------------------------------
# Test Scenarios 11 to 15: Integrations & Strict Unknowns
# ---------------------------------------------------------------------------


def test_scenario_11_unknown_remains_unknown():
    """Scenario 11: UNKNOWN remains UNKNOWN (anti-fabrication principle)."""
    cps = _make_clean_checkpoints()
    cps["CP-008"] = Checkpoint(
        checkpoint_id="CP-008",
        name="Performance Validation",
        category="runtime",
        status=CheckpointStatus.UNKNOWN,
        severity=Severity.INFO,
        findings=[],
        assumptions={"unknown-reason": "No runtime event log available"},
    )
    readiness = _make_dummy_readiness(status=ProductionReadinessStatus.PRODUCTION_READY, score=85.0)
    sufficiency = _make_dummy_sufficiency(decision_sufficiency=False)

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    # Missing evidence must be explicit
    assert any("CP-008" in ev for ev in res.missing_evidence)
    assert res.domain_summaries["runtime"].status == "UNKNOWN"


def test_scenario_12_m5h_confidence_integration():
    """Scenario 12: M5H overall confidence reflected in M5I result."""
    cps = _make_clean_checkpoints()
    readiness = _make_dummy_readiness()
    sufficiency = _make_dummy_sufficiency(overall_confidence=ConfidenceLevel.LOW)

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    assert res.confidence == ConfidenceLevel.LOW


def test_scenario_13_m5h_decision_sufficiency_integration():
    """Scenario 13: M5H decision sufficiency false blocks claim of release certification."""
    cps = _make_clean_checkpoints()
    readiness = _make_dummy_readiness(status=ProductionReadinessStatus.PRODUCTION_READY, score=85.0)
    dec = DecisionSufficiencyRecord(
        decision_name="PRODUCTION_RELEASE",
        domain="readiness",
        decision_status="UNKNOWN",
        is_sufficient=False,
        confidence=ConfidenceLevel.LOW,
    )
    sufficiency = _make_dummy_sufficiency(
        overall_confidence=ConfidenceLevel.LOW,
        decision_sufficiency=False,
        decisions=[dec],
    )

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    assert res.decision_sufficiency is False
    assert res.final_decision == FinalDecisionStatus.INSUFFICIENT_EVIDENCE


def test_scenario_14_m5f_idempotency_risk_integration():
    """Scenario 14: M5F idempotency findings integrate into M5I risk profile and risk chains."""
    cps = _make_clean_checkpoints()
    mock_rf = MagicMock()
    mock_rf.rule_id = "RER-DUP-001"
    mock_rf.title = "Append Write Mode Duplicate Data Risk"
    mock_rf.description = "Unprotected append with job retries enabled"
    mock_rf.severity = Severity.HIGH
    mock_rf.status = CheckpointStatus.FAIL
    mock_rf.evidence = ["write_mode=append", "retry_limit=3"]
    mock_rerun = MagicMock()
    mock_rerun.findings = [mock_rf]
    mock_rerun.duplicate_risk = MagicMock(status=CheckpointStatus.FAIL)

    readiness = _make_dummy_readiness(status=ProductionReadinessStatus.NOT_PRODUCTION_READY, score=72.0)

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        rerun_analysis=mock_rerun,
    )
    res = analyzer.analyze()
    assert any(r.category == RiskCategory.DUPLICATE_DATA for r in res.all_risks)
    assert any(c.chain_id == "CHAIN-001" for c in res.risk_chains)


def test_scenario_15_m5g_alignment_drift_integration():
    """Scenario 15: M5G configuration drift integrates into M5I risks, blockers, and chains."""
    cps = _make_clean_checkpoints()
    mock_dim = MagicMock()
    mock_dim.drift_detected = True
    mock_dim.drift_severity = "BLOCKING"
    mock_dim.expected_summary = "15.4.x-scala2.12"
    mock_dim.implemented_summary = "14.3.x-scala2.12"
    mock_dim.actual_summary = "14.3.x-scala2.12"
    mock_dim.findings = []
    mock_align = MagicMock()
    mock_align.dimensions = {"compute_runtime": mock_dim}

    readiness = _make_dummy_readiness(status=ProductionReadinessStatus.NOT_PRODUCTION_READY, score=78.0)

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        alignment_analysis=mock_align,
    )
    res = analyzer.analyze()
    assert any(r.category == RiskCategory.CONFIGURATION_DRIFT for r in res.all_risks)
    assert any("COMPUTE_RUNTIME" in b.blocker_id for b in res.blockers)
    assert any(c.chain_id == "CHAIN-003" for c in res.risk_chains)


# ---------------------------------------------------------------------------
# Test Scenarios 16 to 20: Scoring, JSON, CLI, and Pipelines
# ---------------------------------------------------------------------------


def test_scenario_16_cp024_readiness_integration():
    """Scenario 16: CP-024 production readiness integration preserves semantics."""
    cps = _make_clean_checkpoints()
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.NOT_PRODUCTION_READY,
        score=85.0,
        decision_reasons=["Policy gate failed on reliability"],
    )

    analyzer = DecisionRiskSynthesisAnalyzer(checkpoints=cps, readiness=readiness)
    res = analyzer.analyze()
    assert res.final_decision == FinalDecisionStatus.NOT_PRODUCTION_READY
    assert any("reliability" in exp for exp in res.decision_explanation)


def test_scenario_17_score_cannot_override_blocker():
    """Scenario 17: Score (e.g. 88/100) cannot override blocker; score_override_reason generated."""
    cps = _make_clean_checkpoints()
    crit = _make_finding(
        rule_id="SEC-001",
        name="Exposed Access Token",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        category="security",
        blocking=True,
    )
    # High score (88/100)
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.NOT_PRODUCTION_READY,
        score=88.5,
        blocking_findings=[crit],
    )

    analyzer = DecisionRiskSynthesisAnalyzer(checkpoints=cps, readiness=readiness)
    res = analyzer.analyze()
    assert res.final_decision == FinalDecisionStatus.NOT_PRODUCTION_READY
    assert res.score_override_reason is not None
    assert "88.5/100" in res.score_override_reason
    assert "Exposed Access Token" in res.score_override_reason


def test_scenario_18_json_serialization():
    """Scenario 18: JSON serialization roundtrip."""
    cps = _make_clean_checkpoints()
    readiness = _make_dummy_readiness()
    sufficiency = _make_dummy_sufficiency()

    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=cps,
        readiness=readiness,
        evidence_sufficiency=sufficiency,
    )
    res = analyzer.analyze()
    res_dict = res.to_dict()
    res_json = json.dumps(res_dict)
    loaded = json.loads(res_json)
    assert loaded["final_decision"] == "PRODUCTION_READY"
    assert "all_risks" in loaded
    assert "blockers" in loaded
    assert "domain_summaries" in loaded
    assert len(loaded["domain_summaries"]) == 16


def test_scenario_19_cli_rendering(repo_root):
    """Scenario 19: CLI rendering verification contains M5I section."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(repo_root / "tests" / "fixtures" / "contracts" / "small_batch_pipeline.yaml"),
            "--offline",
        ],
    )
    assert result.exit_code == 0
    assert "DECISION & RISK SYNTHESIS (M5I)" in result.output
    assert "FINAL DECISION:" in result.output
    assert "CONFIDENCE:" in result.output
    assert "DECISION SUFFICIENCY:" in result.output


@pytest.mark.parametrize(
    "fixture_name",
    [
        "small_batch_pipeline.yaml",
        "incremental_pipeline.yaml",
        "rerun_retry_risk_pipeline.yaml",
        "merge_upsert_pipeline.yaml",
        "concurrency_retry_failure_pipeline.yaml",
    ],
)
def test_scenario_20_representative_pipelines(repo_root, fixture_name):
    """Scenario 20: Validation on all five representative pipelines."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "--contract",
            str(repo_root / "tests" / "fixtures" / "contracts" / fixture_name),
            "--offline",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "decision_risk_synthesis" in payload
    drs = payload["decision_risk_synthesis"]
    assert "final_decision" in drs
    assert drs["final_decision"] in [m.value for m in FinalDecisionStatus]
    assert "confidence" in drs
    assert "decision_sufficiency" in drs
    assert "all_risks" in drs
    assert "domain_summaries" in drs
