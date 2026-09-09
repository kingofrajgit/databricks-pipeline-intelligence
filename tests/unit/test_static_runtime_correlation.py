"""Unit tests for Phase 7 Static Risk to Runtime Evidence Correlation Engine."""

from __future__ import annotations

from datetime import UTC, datetime

from dpif.models import CheckpointStatus, EvidenceRecord, Finding, Severity
from dpif.runtime.correlation import correlate_static_and_runtime


def _make_finding(rule_id: str, status: CheckpointStatus, category: str = "code") -> Finding:
    return Finding(
        finding_id=f"{rule_id}-finding",
        rule_id=rule_id,
        name=f"Finding for {rule_id}",
        title=f"Finding for {rule_id}",
        description="Test finding",
        category=category,
        pipeline_name="test_pipeline",
        status=status,
        severity=Severity.MEDIUM,
        timestamp=datetime.now(UTC),
        evidence=EvidenceRecord(
            rule_id=rule_id,
            status=status,
            severity=Severity.MEDIUM,
            observed={},
            expected={},
            evidence=["test evidence"],
            recommendation="test rec",
            confidence=0.8,
        ),
    )


def test_correlation_confirmed_single_runtime():
    # 1 static finding + 1 matching runtime finding -> STATIC_RISK_CONFIRMED
    static_findings = [_make_finding("CODE-PYSPARK-008", CheckpointStatus.WARN)]
    runtime_findings = [_make_finding("RUNTIME-PERF-002", CheckpointStatus.WARN, "performance")]

    results = correlate_static_and_runtime(
        static_findings, runtime_findings, has_runtime_evidence=True
    )
    assert len(results) == 1
    c = results[0]
    assert c.static_rule_id == "CODE-PYSPARK-008"
    assert c.runtime_rule_ids == ["RUNTIME-PERF-002"]
    assert c.status == "STATIC_RISK_CONFIRMED"
    assert c.confidence == 0.9


def test_correlation_supports_multiple_runtime():
    # 1 static finding + multiple matching runtime findings -> RUNTIME_SUPPORTS_STATIC_RISK
    static_findings = [_make_finding("CODE-PYSPARK-008", CheckpointStatus.WARN)]
    runtime_findings = [
        _make_finding("RUNTIME-PERF-002", CheckpointStatus.WARN, "performance"),
        _make_finding("RUNTIME-PERF-003", CheckpointStatus.FAIL, "performance"),
    ]

    results = correlate_static_and_runtime(
        static_findings, runtime_findings, has_runtime_evidence=True
    )
    assert len(results) == 1
    c = results[0]
    assert c.static_rule_id == "CODE-PYSPARK-008"
    assert set(c.runtime_rule_ids) == {"RUNTIME-PERF-002", "RUNTIME-PERF-003"}
    assert c.status == "RUNTIME_SUPPORTS_STATIC_RISK"
    assert c.confidence == 0.95


def test_correlation_not_observed_in_run():
    # Static finding present, runtime run provided, but no anomaly triggered
    static_findings = [_make_finding("CODE-PYSPARK-008", CheckpointStatus.WARN)]
    runtime_findings = []  # nominal execution

    results = correlate_static_and_runtime(
        static_findings, runtime_findings, has_runtime_evidence=True
    )
    assert len(results) == 1
    c = results[0]
    assert c.static_rule_id == "CODE-PYSPARK-008"
    assert c.status == "STATIC_RISK_NOT_OBSERVED_IN_SUPPLIED_RUN"
    assert c.confidence == 0.75


def test_correlation_runtime_evidence_unavailable():
    # Static finding present, but no runtime execution evidence supplied
    static_findings = [_make_finding("CODE-PYSPARK-008", CheckpointStatus.WARN)]
    runtime_findings = []

    results = correlate_static_and_runtime(
        static_findings, runtime_findings, has_runtime_evidence=False
    )
    assert len(results) == 1
    c = results[0]
    assert c.static_rule_id == "CODE-PYSPARK-008"
    assert c.status == "RUNTIME_EVIDENCE_UNAVAILABLE"
    assert c.confidence == 0.5


def test_correlation_sql_and_immutability():
    # SQL finding correlation + verify static finding is unmodified
    sql_finding = _make_finding("CODE-SQL-003", CheckpointStatus.WARN)
    initial_status = sql_finding.status
    initial_severity = sql_finding.severity

    runtime_findings = [_make_finding("RUNTIME-PERF-002", CheckpointStatus.WARN, "performance")]

    results = correlate_static_and_runtime(
        [sql_finding], runtime_findings, has_runtime_evidence=True
    )
    assert len(results) == 1
    assert results[0].static_rule_id == "CODE-SQL-003"
    assert results[0].status == "STATIC_RISK_CONFIRMED"

    # Verify static finding was not mutated
    assert sql_finding.status == initial_status
    assert sql_finding.severity == initial_severity


def test_correlation_non_mapped_rule():
    # Rule with no runtime mapping should not produce correlation
    static_findings = [_make_finding("CODE-PYSPARK-999", CheckpointStatus.WARN)]
    results = correlate_static_and_runtime(static_findings, [], has_runtime_evidence=True)
    assert len(results) == 0
