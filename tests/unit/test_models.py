"""Pydantic v2 model tests — every core model must accept keyword construction."""

from __future__ import annotations

import pytest

from dpif.models import (
    AnalysisMethod,
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    EvidenceRecord,
    Finding,
    PipelineContract,
    ReliabilityRules,
    Rule,
    ScalabilityRules,
    ScheduleRules,
    Severity,
    SLARules,
    Source,
    SourceType,
    Target,
    ValidationRun,
)


def test_source_keyword_construction():
    s = Source(source_id="source-001", name="customer-source", type="adls", format="parquet")
    assert s.source_id == "source-001"
    assert s.type == SourceType.ADLS
    assert s.format == "parquet"
    assert s.is_cloud is True
    assert s.is_local is False


def test_source_enum_object_construction():
    s = Source(source_id="s2", type=SourceType.S3, path="s3://bucket/", format="json")
    assert s.type == SourceType.S3
    assert s.path == "s3://bucket/"


def test_source_defaults():
    s = Source(source_id="s3", type="local_file")
    assert s.config == {}
    assert s.data_profile is None
    assert s.path == ""
    assert s.is_local is True


def test_target_keyword_construction():
    t = Target(target_id="t1", type="delta", catalog="prod", schema="marts", path="/delta/t")
    assert t.target_id == "t1"
    assert t.schema_text == "marts"
    assert t.to_dict()["schema"] == "marts"


def test_data_profile_keyword_construction():
    dp = DataProfile(
        total_bytes=10 * 1024**3,
        total_gb=10.0,
        file_count=100,
        average_file_size_kb=104857.6,
        record_count=1000,
    )
    assert dp.has_sufficient_metadata is True
    assert dp.to_dict()["file_count"] == 100


def test_data_profile_empty_is_insufficient():
    assert DataProfile().has_sufficient_metadata is False


def test_checkpoint_keyword_construction():
    cp = Checkpoint(
        checkpoint_id="CP-001",
        name="Source Validation",
        category="source",
        status=CheckpointStatus.PASS,
    )
    assert cp.status == CheckpointStatus.PASS
    assert cp.findings == []
    assert cp.depends_on == []


def test_checkpoint_default_evidence_is_valid():
    # Regression: default_factory=EvidenceRecord used to crash (missing fields).
    cp = Checkpoint(checkpoint_id="CP-001", name="X", category="source")
    assert cp.status == CheckpointStatus.UNKNOWN
    assert cp.evidence.status == CheckpointStatus.UNKNOWN


def test_rule_keyword_construction():
    r = Rule(
        rule_id="CODE-PYSPARK-001",
        name="Driver Collection Detection",
        category="code",
        severity=Severity.CRITICAL,
        description="Detect driver-side collection.",
        condition=r"\.collect\s*\(",
    )
    assert r.blocking is False
    assert r.version == "1.0.0"
    assert r.context_aware is True


def test_evidence_keyword_construction():
    ev = EvidenceRecord(
        rule_id="R1",
        status=CheckpointStatus.FAIL,
        severity=Severity.HIGH,
        observed={"a": 1},
        expected={"a": 0},
        evidence=["f.py:1"],
        recommendation="fix",
        confidence=0.9,
    )
    d = ev.to_dict()
    assert d["rule_id"] == "R1"
    assert d["status"] == "FAIL"
    assert d["confidence"] == 0.9


def test_finding_structured_fields():
    ev = EvidenceRecord(
        rule_id="R1",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        recommendation="rec",
        confidence=0.95,
    )
    f = Finding(
        finding_id="f1",
        rule_id="R1",
        name="N",
        category="code",
        status=CheckpointStatus.FAIL,
        severity=Severity.CRITICAL,
        pipeline_name="p",
        evidence=ev,
        title="T",
        description="D",
        recommendation="R",
        confidence=0.95,
        blocking=True,
    )
    d = f.to_dict()
    for key in (
        "rule_id",
        "category",
        "severity",
        "status",
        "title",
        "description",
        "evidence",
        "recommendation",
        "confidence",
        "blocking",
    ):
        assert key in d, f"missing finding field {key}"
    assert d["blocking"] is True


def test_pipeline_contract_keyword_construction(sample_source, sample_target):
    pc = PipelineContract(
        contract_id="c1", pipeline_name="customer_daily", source=sample_source, target=sample_target
    )
    assert pc.environment == "production"
    assert pc.sla.max_runtime_minutes == 60.0
    assert pc.reliability.retry_count == 2
    assert pc.to_dict()["pipeline_name"] == "customer_daily"


def test_pipeline_contract_validate_flags_bad_volume(sample_source, sample_target):
    pc = PipelineContract(
        contract_id="c1",
        pipeline_name="p",
        source=sample_source,
        target=sample_target,
        expected_daily_volume_gb=0.0,
    )
    assert any("volume" in i.lower() for i in pc.validate_contract())


def test_sla_schedule_reliability_scalability_defaults():
    assert SLARules().to_dict()["max_runtime_minutes"] == 60.0
    assert ScheduleRules().to_dict()["frequency"] == "daily"
    assert ReliabilityRules().to_dict()["retry_count"] == 2
    assert ScalabilityRules().to_dict()["forecast_horizon_days"] == 365


def test_validation_run_model(sample_source, sample_target):
    pc = PipelineContract(
        contract_id="c1", pipeline_name="p", source=sample_source, target=sample_target
    )
    run = ValidationRun(
        run_id="run-1",
        pipeline_name="p",
        contract_id="c1",
        mode="offline",
        status="COMPLETED",
        overall_score=77.0,
    )
    assert run.to_dict()["overall_score"] == 77.0
    assert pc.pipeline_name == "p"


def test_enums_cover_required_values():
    assert CheckpointStatus("UNKNOWN") is CheckpointStatus.UNKNOWN
    assert Severity("CRITICAL") is Severity.CRITICAL
    assert AnalysisMethod("metadata") is AnalysisMethod.METADATA
    assert SourceType("adls") is SourceType.ADLS
    with pytest.raises(ValueError):
        Severity("NOPE")
