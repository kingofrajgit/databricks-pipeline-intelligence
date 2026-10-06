"""Phase 7 regression tests: decision-synthesis evidence integrity.

Proves that an UNKNOWN runtime/scalability condition (missing telemetry)
cannot be laundered into an evidence-backed HIGH XDOM risk and then into
a blocking P0 M5I decision driver — while genuine static and runtime
risks keep working and evidence gaps stay visible.
"""

from __future__ import annotations

from dpif.analyzers.synthesis import DecisionRiskSynthesisAnalyzer
from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    EvidenceRecord,
    Finding,
    Severity,
)
from dpif.readiness.models import CrossDomainRisk, ProductionReadinessStatus
from dpif.readiness.synthesizer import synthesize_cross_domain_risks
from tests.unit.test_decision_synthesis import (
    _make_clean_checkpoints,
    _make_dummy_readiness,
    _make_dummy_sufficiency,
)
from tests.unit.test_production_readiness import _dummy_contract, _dummy_profile


def _finding(
    rule_id: str,
    status: CheckpointStatus,
    severity: Severity = Severity.MEDIUM,
    confidence: float = 0.9,
) -> Finding:
    ev = EvidenceRecord(
        rule_id=rule_id,
        status=status,
        severity=severity,
        confidence=confidence,
        evidence=["test evidence"],
    )
    return Finding(
        finding_id=f"f-{rule_id}",
        rule_id=rule_id,
        name=rule_id,
        title=rule_id,
        category="test",
        status=status,
        severity=severity,
        pipeline_name="p",
        evidence=ev,
        confidence=confidence,
    )


def _cps_with(*findings: Finding) -> dict[str, Checkpoint]:
    cp = Checkpoint(
        checkpoint_id="CP-010",
        name="Scalability Validation",
        category="scalability",
        status=CheckpointStatus.UNKNOWN,
        severity=Severity.INFO,
    )
    cp.findings = list(findings)
    return {"CP-010": cp}


# A. UNKNOWN SCALABILITY-007 (telemetry absent) must not create HIGH XDOM-003.
def test_unknown_scalability_007_creates_no_high_xdom():
    unknown = _finding(
        "SCALABILITY-007",
        CheckpointStatus.UNKNOWN,
        Severity.INFO,
        confidence=0.0,
    )
    risks = synthesize_cross_domain_risks(_cps_with(unknown))
    assert all(r.risk_id != "XDOM-003" for r in risks)


# B. Missing telemetry must not fabricate runtime confidence or provenance.
def test_missing_telemetry_produces_no_runtime_labels():
    unknown = _finding(
        "SCALABILITY-007",
        CheckpointStatus.UNKNOWN,
        Severity.INFO,
        confidence=0.0,
    )
    risks = synthesize_cross_domain_risks(_cps_with(unknown))
    for r in risks:
        assert "Runtime Telemetry" not in r.evidence_sources
        assert r.confidence < 0.5 or r.severity not in ("HIGH", "CRITICAL")


# C. The evidence gap stays visible (finding preserved, no XDOM cover-up).
def test_unknown_gap_remains_visible():
    unknown = _finding(
        "SCALABILITY-007",
        CheckpointStatus.UNKNOWN,
        Severity.INFO,
        confidence=0.0,
    )
    cps = _cps_with(unknown)
    synthesize_cross_domain_risks(cps)
    kept = [f for cp in cps.values() for f in cp.findings if f.rule_id == "SCALABILITY-007"]
    assert len(kept) == 1
    assert kept[0].status == CheckpointStatus.UNKNOWN


# D. Genuine static risk still produces its XDOM finding.
def test_static_collect_risk_still_detected():
    collect = _finding("CODE-PYSPARK-006", CheckpointStatus.FAIL, Severity.HIGH)
    cp = Checkpoint(
        checkpoint_id="CP-004",
        name="Code Validation",
        category="code",
        status=CheckpointStatus.FAIL,
        severity=Severity.HIGH,
    )
    cp.findings = [collect]
    risks = synthesize_cross_domain_risks(
        {"CP-004": cp},
        contract=_dummy_contract(daily_gb=500.0),
        profile=_dummy_profile(500.0),
    )
    assert any(r.risk_id == "XDOM-001" for r in risks)


# E. Genuine runtime evidence still produces HIGH/blocking-capable XDOM-003.
def test_measured_shuffle_still_produces_xdom003():
    shuffle = _finding(
        "RUNTIME-PERF-002", CheckpointStatus.FAIL, Severity.HIGH, confidence=0.95
    )
    risks = synthesize_cross_domain_risks(_cps_with(shuffle))
    xdom = next(r for r in risks if r.risk_id == "XDOM-003")
    assert xdom.severity == "HIGH"
    assert "Runtime Telemetry" in xdom.evidence_sources
    assert "Observed" in xdom.description
    assert xdom.confidence <= 0.85


# E (projection variant). WARN projection keeps XDOM-003 with honest labels.
def test_projected_shuffle_keeps_honest_provenance():
    proj = _finding(
        "SCALABILITY-007", CheckpointStatus.WARN, Severity.MEDIUM, confidence=0.75
    )
    risks = synthesize_cross_domain_risks(_cps_with(proj))
    xdom = next(r for r in risks if r.risk_id == "XDOM-003")
    assert "Runtime Telemetry" not in xdom.evidence_sources
    assert "Linear Projections" in xdom.evidence_sources
    assert "Projected" in xdom.description
    assert xdom.confidence == 0.75


# F. UNKNOWN/missing-evidence XDOM cannot become a P0 blocker (M5I guard).
def test_evidence_absent_xdom_cannot_block():
    phantom = CrossDomainRisk(
        risk_id="XDOM-003",
        title="High Network Shuffle & Spill Risk",
        severity="HIGH",
        contributing_domains=["Performance", "Scalability"],
        evidence_sources=["Runtime Telemetry", "Linear Projections"],
        description="Observed or projected shuffle volume exceeds safe thresholds.",
        recommendation="Enable AQE.",
        confidence=0.0,
    )
    readiness = _make_dummy_readiness()
    readiness.cross_domain_risks = [phantom]
    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=_make_clean_checkpoints(),
        readiness=readiness,
        evidence_sufficiency=_make_dummy_sufficiency(),
    )
    result = analyzer.analyze()
    assert all(b.source != "XDOM-003" for b in result.blockers)
    assert all(r.priority != "P0" or "Shuffle" not in r.title for r in result.remediations)


# F (preservation). Evidence-backed HIGH XDOM still blocks with P0.
def test_evidence_backed_high_xdom_still_blocks():
    genuine = CrossDomainRisk(
        risk_id="XDOM-003",
        title="High Network Shuffle & Spill Risk",
        severity="HIGH",
        contributing_domains=["Performance", "Scalability"],
        evidence_sources=["Runtime Telemetry"],
        description="Observed shuffle volume exceeds safe network thresholds.",
        recommendation="Enable AQE.",
        confidence=0.85,
    )
    readiness = _make_dummy_readiness()
    readiness.cross_domain_risks = [genuine]
    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=_make_clean_checkpoints(),
        readiness=readiness,
        evidence_sufficiency=_make_dummy_sufficiency(),
    )
    result = analyzer.analyze()
    assert any(b.source == "XDOM-003" for b in result.blockers)
    assert any(r.priority == "P0" and "Shuffle" in r.title for r in result.remediations)


# G. Shared synthesis seam: identical evidence, identical XDOM semantics.
# Both offline (cli) and online (orchestration) validations consume the same
# synthesize_cross_domain_risks function, so equivalent evidence states must
# yield equivalent XDOM decisions on either path.
def test_offline_online_synthesis_parity():
    shuffle = _finding(
        "RUNTIME-PERF-002", CheckpointStatus.FAIL, Severity.HIGH, confidence=0.95
    )
    unknown = _finding(
        "SCALABILITY-007",
        CheckpointStatus.UNKNOWN,
        Severity.INFO,
        confidence=0.0,
    )
    offline_risks = synthesize_cross_domain_risks(_cps_with(shuffle, unknown))
    online_risks = synthesize_cross_domain_risks(_cps_with(shuffle, unknown))
    assert [r.risk_id for r in offline_risks] == [r.risk_id for r in online_risks]
    assert [r.severity for r in offline_risks] == [r.severity for r in online_risks]
    assert [r.confidence for r in offline_risks] == [r.confidence for r in online_risks]
    # UNKNOWN-only input yields no XDOM-003 on either path.
    assert all(
        r.risk_id != "XDOM-003"
        for r in synthesize_cross_domain_risks(_cps_with(unknown))
    )


# Sufficiency-FALSE coexists with honest risks (no phantom driver).
def test_insufficient_evidence_without_phantom_blocker():
    unknown = _finding(
        "SCALABILITY-007",
        CheckpointStatus.UNKNOWN,
        Severity.INFO,
        confidence=0.0,
    )
    risks = synthesize_cross_domain_risks(_cps_with(unknown))
    readiness = _make_dummy_readiness(
        status=ProductionReadinessStatus.NOT_PRODUCTION_READY
    )
    analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=_cps_with(unknown),
        readiness=readiness,
        evidence_sufficiency=_make_dummy_sufficiency(decision_sufficiency=False),
    )
    result = analyzer.analyze()
    assert result.decision_sufficiency is False
    assert all("Shuffle" not in b.title for b in result.blockers)
    assert all(r.risk_id != "XDOM-003" for r in risks)
