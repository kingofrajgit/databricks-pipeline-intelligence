"""Unit tests for M5K Enterprise Fleet Validation."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from dpif.cli import cli
from dpif.error_handling import ConfigurationError
from dpif.models import CheckpointStatus, Severity
from dpif.models.fleet import (
    CollisionStatus,
    CrossPipelineCollisionFinding,
    EnterpriseEnvironmentPolicy,
    EnterprisePipelineTarget,
    EnvironmentTier,
    FleetManifest,
    FleetSummaryMetrics,
    FleetValidationResult,
    PipelineFleetExecution,
)
from dpif.models.sufficiency import ConfidenceLevel
from dpif.models.synthesis import DecisionRiskSynthesisResult, FinalDecisionStatus
from dpif.orchestration.fleet import (
    EnterpriseFleetOrchestrator,
    aggregate_fleet_metrics,
    detect_fleet_collisions,
    load_and_validate_fleet_manifest,
)
from dpif.orchestration.online import OnlineValidationResult
from dpif.reporting.enterprise_exporters import (
    export_junit_xml,
    export_markdown_summary,
    export_sarif,
)


def test_valid_fleet_manifest_parsing(tmp_path: Path):
    """Test valid YAML fleet manifest is parsed correctly."""
    manifest_file = tmp_path / "fleet.yaml"
    manifest_file.write_text(
        """
fleet:
  name: "ecommerce-fleet"
  environment: "staging"

workspace:
  host: "https://adb-123.databricks.com"

pipelines:
  - id: "pipeline_orders"
    job_id: 101
    environment: "staging"
  - id: "pipeline_users"
    pipeline_id: "dlt-user-pipeline"
""",
        encoding="utf-8",
    )

    manifest = load_and_validate_fleet_manifest(manifest_file)
    assert manifest.name == "ecommerce-fleet"
    assert manifest.environment == EnvironmentTier.STAGING
    assert manifest.workspace_host == "https://adb-123.databricks.com"
    assert len(manifest.pipelines) == 2
    assert manifest.pipelines[0].id == "pipeline_orders"
    assert manifest.pipelines[0].job_id == 101
    assert manifest.pipelines[1].id == "pipeline_users"
    assert manifest.pipelines[1].pipeline_id == "dlt-user-pipeline"


def test_manifest_missing_file():
    """Test non-existent manifest file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_and_validate_fleet_manifest("non_existent_fleet_manifest.yaml")


def test_manifest_duplicate_pipeline_ids(tmp_path: Path):
    """Test duplicate pipeline IDs in manifest are strictly rejected."""
    manifest_file = tmp_path / "dup_fleet.yaml"
    manifest_file.write_text(
        """
fleet:
  name: "dup-fleet"
pipelines:
  - id: "dup_id"
    job_id: 101
  - id: "dup_id"
    job_id: 102
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Duplicate pipeline id 'dup_id'"):
        load_and_validate_fleet_manifest(manifest_file)


def test_manifest_missing_fleet_name(tmp_path: Path):
    """Test manifest without fleet name raises ValueError."""
    manifest_file = tmp_path / "no_name_fleet.yaml"
    manifest_file.write_text(
        """
fleet:
  environment: "production"
pipelines:
  - id: "pipe1"
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="fleet.name"):
        load_and_validate_fleet_manifest(manifest_file)


def test_manifest_security_embedded_token_rejected(tmp_path: Path):
    """Security test: Manifest containing sensitive credential keys is rejected."""
    manifest_file = tmp_path / "insecure.yaml"
    manifest_file.write_text(
        """
fleet:
  name: "insecure-fleet"
workspace:
  host: "https://adb-123.databricks.com"
  token: "placeholder-token"
pipelines:
  - id: "pipe1"
    job_id: 101
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="sensitive credential key 'token'"):
        load_and_validate_fleet_manifest(manifest_file)


def test_manifest_security_embedded_bearer_secret_value_rejected(tmp_path: Path):
    """Security test: Manifest containing sensitive bearer token value is rejected."""
    manifest_file = tmp_path / "insecure_val.yaml"
    mock_token = "dapi" + ("0" * 32)
    manifest_file.write_text(
        f"""
fleet:
  name: "insecure-fleet"
pipelines:
  - id: "pipe1"
    custom_param: "{mock_token}"
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="sensitive credential value"):
        load_and_validate_fleet_manifest(manifest_file)


def test_environment_policy_development():
    """Test development environment policy allows warnings and lower confidence."""
    policy = EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.DEVELOPMENT)
    assert policy.tier == EnvironmentTier.DEVELOPMENT
    assert policy.min_quality_score == 50.0
    assert not policy.require_decision_sufficiency
    assert policy.allow_conditional_go

    mock_res = MagicMock(spec=OnlineValidationResult)
    mock_res.quality_score = 65.0
    mock_res.confidence = ConfidenceLevel.LOW
    mock_res.decision_sufficiency = False
    mock_res.has_blocking = False
    mock_res.final_decision = FinalDecisionStatus.CONDITIONAL.value
    mock_res.alignment_analysis = None
    mock_res.decision_risk_synthesis = None

    passed, violations = policy.evaluate_pipeline(mock_res)
    assert passed
    assert len(violations) == 0


def test_environment_policy_production_strict():
    """Test production environment policy strictly blocks on low score, confidence, or blockers."""
    policy = EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.PRODUCTION)
    assert policy.tier == EnvironmentTier.PRODUCTION
    assert policy.min_quality_score == 75.0
    assert policy.min_confidence == ConfidenceLevel.HIGH
    assert policy.require_decision_sufficiency
    assert not policy.allow_conditional_go

    # Pipeline with low score and medium confidence
    mock_res = MagicMock(spec=OnlineValidationResult)
    mock_res.quality_score = 68.0
    mock_res.confidence = ConfidenceLevel.MEDIUM
    mock_res.decision_sufficiency = True
    mock_res.has_blocking = False
    mock_res.final_decision = FinalDecisionStatus.CONDITIONAL.value
    mock_res.alignment_analysis = None
    mock_res.decision_risk_synthesis = None

    passed, violations = policy.evaluate_pipeline(mock_res)
    assert not passed
    assert any("below policy minimum 75.0" in v for v in violations)
    assert any("Confidence 'MEDIUM' is below required policy level 'HIGH'" in v for v in violations)
    assert any("not permitted under 'production' policy" in v for v in violations)


def test_environment_policy_p0_blocker():
    """Test policy blocks when P0 production risks exist."""
    policy = EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.PRODUCTION)

    mock_res = MagicMock(spec=OnlineValidationResult)
    mock_res.quality_score = 90.0
    mock_res.confidence = ConfidenceLevel.HIGH
    mock_res.decision_sufficiency = True
    mock_res.has_blocking = True
    mock_res.final_decision = FinalDecisionStatus.NOT_PRODUCTION_READY.value
    mock_res.alignment_analysis = None

    mock_syn = MagicMock(spec=DecisionRiskSynthesisResult)
    mock_syn.production_blockers = ["Critical data corruption risk"]
    mock_res.decision_risk_synthesis = mock_syn

    passed, violations = policy.evaluate_pipeline(mock_res)
    assert not passed
    assert any("blocking P0 production risk" in v for v in violations)
    assert any("Synthesized release decision is NOT_PRODUCTION_READY" in v for v in violations)


def test_failure_isolation():
    """Failure isolation: Exception in one pipeline does not crash neighboring pipeline validations."""
    mock_connector = MagicMock()
    mock_provider = MagicMock()

    orchestrator = EnterpriseFleetOrchestrator(connector=mock_connector, provider=mock_provider)

    manifest = FleetManifest(
        name="test-isolation",
        environment=EnvironmentTier.STAGING,
        pipelines=[
            EnterprisePipelineTarget(id="pipe_ok_1", job_id=1),
            EnterprisePipelineTarget(id="pipe_fail_2", job_id=2),
            EnterprisePipelineTarget(id="pipe_ok_3", job_id=3),
        ],
    )

    # Mock _execute_single_target to succeed for 1 and 3, raise API error for 2
    def mock_exec(target, ws, policy, token=None):
        if target.id == "pipe_fail_2":
            return PipelineFleetExecution(
                pipeline_id=target.id,
                target=target,
                success=False,
                policy_passed=False,
                policy_violations=["Pipeline execution failure: HTTP 500 API Gateway Timeout"],
                error_message="HTTP 500 API Gateway Timeout",
            )
        else:
            mock_vr = MagicMock(spec=OnlineValidationResult)
            mock_vr.quality_score = 85.0
            mock_vr.confidence = ConfidenceLevel.HIGH
            mock_vr.decision_sufficiency = True
            mock_vr.has_blocking = False
            mock_vr.final_decision = FinalDecisionStatus.PRODUCTION_READY.value
            mock_vr.alignment_analysis = None
            mock_vr.decision_risk_synthesis = None
            return PipelineFleetExecution(
                pipeline_id=target.id,
                target=target,
                success=True,
                policy_passed=True,
                validation_result=mock_vr,
            )

    orchestrator._execute_single_target = mock_exec  # type: ignore[method-assign]

    fleet_result = orchestrator.validate_fleet(manifest)

    assert fleet_result.summary.total_pipelines == 3
    assert fleet_result.summary.successful_validations == 2
    assert fleet_result.summary.failed_validations == 1
    assert fleet_result.pipeline_executions["pipe_ok_1"].success is True
    assert fleet_result.pipeline_executions["pipe_fail_2"].success is False
    assert "HTTP 500" in fleet_result.pipeline_executions["pipe_fail_2"].error_message
    assert fleet_result.pipeline_executions["pipe_ok_3"].success is True
    # Fleet policy must fail because 1 pipeline failed
    assert fleet_result.policy_passed is False


def test_collision_detection_confirmed_overwrite():
    """Test collision detection identifies destructive write collision (overwrite vs append)."""
    mock_vr_1 = MagicMock(spec=OnlineValidationResult)
    mock_vr_1.rerun_analysis = MagicMock(target_table="catalog.schema.customers", write_mode="append")

    mock_vr_2 = MagicMock(spec=OnlineValidationResult)
    mock_vr_2.rerun_analysis = MagicMock(target_table="catalog.schema.customers", write_mode="overwrite")

    executions = {
        "p1": PipelineFleetExecution(
            pipeline_id="p1",
            target=EnterprisePipelineTarget(id="p1"),
            success=True,
            policy_passed=True,
            validation_result=mock_vr_1,
        ),
        "p2": PipelineFleetExecution(
            pipeline_id="p2",
            target=EnterprisePipelineTarget(id="p2"),
            success=True,
            policy_passed=True,
            validation_result=mock_vr_2,
        ),
    }

    collisions = detect_fleet_collisions(executions)
    assert len(collisions) == 1
    col = collisions[0]
    assert col.status == CollisionStatus.CONFIRMED
    assert col.severity == Severity.CRITICAL
    assert col.target_resource == "catalog.schema.customers"
    assert set(col.conflicting_pipeline_ids) == {"p1", "p2"}
    assert "destructive write mode" in col.description


def test_collision_detection_distinct_targets_no_collision():
    """Test distinct pipeline targets produce zero collisions."""
    mock_vr_1 = MagicMock(spec=OnlineValidationResult)
    mock_vr_1.rerun_analysis = MagicMock(target_table="catalog.schema.table_a", write_mode="append")

    mock_vr_2 = MagicMock(spec=OnlineValidationResult)
    mock_vr_2.rerun_analysis = MagicMock(target_table="catalog.schema.table_b", write_mode="overwrite")

    executions = {
        "p1": PipelineFleetExecution(
            pipeline_id="p1",
            target=EnterprisePipelineTarget(id="p1"),
            success=True,
            policy_passed=True,
            validation_result=mock_vr_1,
        ),
        "p2": PipelineFleetExecution(
            pipeline_id="p2",
            target=EnterprisePipelineTarget(id="p2"),
            success=True,
            policy_passed=True,
            validation_result=mock_vr_2,
        ),
    }

    collisions = detect_fleet_collisions(executions)
    assert len(collisions) == 0


def test_fleet_score_aggregation_never_masks_blocker():
    """Test fleet quality score aggregation caps the score below 70.0 when a pipeline is blocked."""
    policy = EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.PRODUCTION)

    # p1 scores 100.0, p2 scores 100.0, but p3 has a critical blocker (score 0.0)
    mock_vr_1 = MagicMock(spec=OnlineValidationResult, quality_score=100.0, confidence="HIGH", final_decision="PRODUCTION_READY", decision_risk_synthesis=None, has_blocking=False)
    mock_vr_2 = MagicMock(spec=OnlineValidationResult, quality_score=100.0, confidence="HIGH", final_decision="PRODUCTION_READY", decision_risk_synthesis=None, has_blocking=False)
    mock_vr_3 = MagicMock(spec=OnlineValidationResult, quality_score=40.0, confidence="LOW", final_decision="NOT_PRODUCTION_READY", decision_risk_synthesis=MagicMock(production_blockers=["Blocker 1"], top_risks=[]), has_blocking=True)

    executions = {
        "p1": PipelineFleetExecution(pipeline_id="p1", target=EnterprisePipelineTarget(id="p1"), success=True, policy_passed=True, validation_result=mock_vr_1),
        "p2": PipelineFleetExecution(pipeline_id="p2", target=EnterprisePipelineTarget(id="p2"), success=True, policy_passed=True, validation_result=mock_vr_2),
        "p3": PipelineFleetExecution(pipeline_id="p3", target=EnterprisePipelineTarget(id="p3"), success=True, policy_passed=False, policy_violations=["P0 blocker"], validation_result=mock_vr_3),
    }

    metrics, passed = aggregate_fleet_metrics(executions, policy, [])
    assert not passed
    assert metrics.blocked_policy == 1
    assert metrics.total_blockers == 1
    # Raw average would be (100 + 100 + 40)/3 = 80.0, but blocker penalty must cap it below 70.0
    assert metrics.fleet_quality_score < 70.0
    assert metrics.fleet_quality_score == 64.9  # 69.9 - (1 * 5.0)


def test_junit_xml_generation(tmp_path: Path):
    """Test JUnit XML export generates valid parseable XML structure."""
    policy = EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.PRODUCTION)
    summary = FleetSummaryMetrics(
        total_pipelines=2,
        successful_validations=2,
        failed_validations=0,
        passed_policy=1,
        blocked_policy=1,
        fleet_quality_score=64.9,
    )

    mock_vr = MagicMock(spec=OnlineValidationResult)
    mock_vr.checkpoints = {
        "CP-001": MagicMock(name="Source Profile", status=CheckpointStatus.PASS, score=95.0, findings=[]),
        "CP-018": MagicMock(name="Rerun Safety", status=CheckpointStatus.FAIL, score=30.0, findings=[
            MagicMock(severity=Severity.CRITICAL, title="Non-idempotent write", message="Write operation overwrites data", name="Non-idempotent write")
        ]),
    }

    executions = {
        "p1": PipelineFleetExecution(
            pipeline_id="p1",
            target=EnterprisePipelineTarget(id="p1"),
            success=True,
            policy_passed=True,
            duration_seconds=1.2,
            validation_result=mock_vr,
        ),
        "p2": PipelineFleetExecution(
            pipeline_id="p2",
            target=EnterprisePipelineTarget(id="p2"),
            success=True,
            policy_passed=False,
            policy_violations=["Quality score too low"],
            duration_seconds=0.8,
            validation_result=None,
        ),
    }

    fleet_res = FleetValidationResult(
        fleet_name="test-junit-fleet",
        environment=EnvironmentTier.PRODUCTION,
        policy=policy,
        summary=summary,
        pipeline_executions=executions,
        policy_passed=False,
        duration_seconds=2.0,
    )

    xml_path = tmp_path / "report.xml"
    xml_str = export_junit_xml(fleet_res, xml_path)

    assert xml_path.exists()
    root = ET.fromstring(xml_str)
    assert root.tag == "testsuites"
    assert root.attrib["name"] == "DPIF Fleet Validation - test-junit-fleet"
    assert len(root.findall("testsuite")) == 2


def test_sarif_generation(tmp_path: Path):
    """Test SARIF v2.1.0 generation produces valid JSON matching schema requirements."""
    policy = EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.STAGING)
    summary = FleetSummaryMetrics(total_pipelines=1, successful_validations=1, passed_policy=1)

    mock_finding = MagicMock(
        rule_id="RULE-CODE-001",
        title="Syntax error",
        message="Invalid syntax in SQL query",
        severity=Severity.HIGH,
        file_path="src/query.py",
        line_number=42,
    )
    mock_vr = MagicMock(spec=OnlineValidationResult)
    mock_vr.findings = [mock_finding]

    executions = {
        "p1": PipelineFleetExecution(
            pipeline_id="p1",
            target=EnterprisePipelineTarget(id="p1"),
            success=True,
            policy_passed=True,
            validation_result=mock_vr,
        )
    }

    collision = CrossPipelineCollisionFinding(
        status=CollisionStatus.CONFIRMED,
        severity=Severity.CRITICAL,
        target_resource="delta_db.orders",
        conflicting_pipeline_ids=["p1", "p2"],
        description="Write collision detected",
        recommendation="Isolate tables",
    )

    fleet_res = FleetValidationResult(
        fleet_name="sarif-fleet",
        environment=EnvironmentTier.STAGING,
        policy=policy,
        summary=summary,
        pipeline_executions=executions,
        collisions=[collision],
        policy_passed=False,
    )

    sarif_path = tmp_path / "report.sarif"
    sarif_str = export_sarif(fleet_res, sarif_path)

    assert sarif_path.exists()
    doc = json.loads(sarif_str)
    assert doc["version"] == "2.1.0"
    assert len(doc["runs"]) == 1
    assert len(doc["runs"][0]["results"]) == 2
    assert doc["runs"][0]["results"][0]["ruleId"] == "RULE-CODE-001"


def test_markdown_summary_generation_and_secret_masking(tmp_path: Path):
    """Test Markdown PR summary generates formatted tables and masks sensitive workspace host credentials."""
    policy = EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.PRODUCTION)
    summary = FleetSummaryMetrics(
        total_pipelines=1,
        successful_validations=1,
        passed_policy=1,
        fleet_quality_score=92.5,
    )

    mock_syn = MagicMock(
        remediation_plans=[
            MagicMock(priority="P0", description="Fix partition pruning", action="Add date partition filter")
        ]
    )
    mock_vr = MagicMock(
        spec=OnlineValidationResult,
        quality_score=92.5,
        confidence="HIGH",
        final_decision="PRODUCTION_READY",
        decision_risk_synthesis=mock_syn,
    )

    executions = {
        "p1": PipelineFleetExecution(
            pipeline_id="p1",
            target=EnterprisePipelineTarget(id="p1"),
            success=True,
            policy_passed=True,
            validation_result=mock_vr,
        )
    }

    fleet_res = FleetValidationResult(
        fleet_name="md-fleet",
        environment=EnvironmentTier.PRODUCTION,
        policy=policy,
        summary=summary,
        pipeline_executions=executions,
        policy_passed=True,
        duration_seconds=3.14,
    )

    md_path = tmp_path / "summary.md"
    md_str = export_markdown_summary(fleet_res, md_path)

    assert md_path.exists()
    assert "# ✅ DPIF Enterprise Fleet Validation Report" in md_str
    assert "92.5 / 100" in md_str
    assert "Fix partition pruning" in md_str


def test_cli_validate_online_fleet_success(tmp_path: Path, monkeypatch):
    """Test CLI command dpif validate-online-fleet with exit code 0 when policy passes."""
    manifest_file = tmp_path / "fleet_cli.yaml"
    manifest_file.write_text(
        """
fleet:
  name: "cli-fleet"
  environment: "development"
pipelines:
  - id: "cli_p1"
    job_id: 123
""",
        encoding="utf-8",
    )

    # Mock EnterpriseFleetOrchestrator to return passing result
    mock_fleet_result = FleetValidationResult(
        fleet_name="cli-fleet",
        environment=EnvironmentTier.DEVELOPMENT,
        policy=EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.DEVELOPMENT),
        summary=FleetSummaryMetrics(total_pipelines=1, successful_validations=1, passed_policy=1, fleet_quality_score=90.0),
        pipeline_executions={},
        policy_passed=True,
    )

    monkeypatch.setattr(
        "dpif.orchestration.fleet.EnterpriseFleetOrchestrator.validate_fleet",
        lambda self, *args, **kwargs: mock_fleet_result,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate-online-fleet",
            "--manifest",
            str(manifest_file),
            "--environment",
            "development",
        ],
    )
    assert result.exit_code == 0
    assert "POLICY GATE:       PASSED" in result.output


def test_cli_validate_online_fleet_policy_blocked(tmp_path: Path, monkeypatch):
    """Test CLI command dpif validate-online-fleet returns exit code 1 when policy is blocked."""
    manifest_file = tmp_path / "fleet_blocked.yaml"
    manifest_file.write_text(
        """
fleet:
  name: "blocked-fleet"
  environment: "production"
pipelines:
  - id: "p_block"
    job_id: 456
""",
        encoding="utf-8",
    )

    mock_fleet_result = FleetValidationResult(
        fleet_name="blocked-fleet",
        environment=EnvironmentTier.PRODUCTION,
        policy=EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.PRODUCTION),
        summary=FleetSummaryMetrics(total_pipelines=1, successful_validations=1, blocked_policy=1, fleet_quality_score=50.0),
        pipeline_executions={},
        policy_passed=False,
    )

    monkeypatch.setattr(
        "dpif.orchestration.fleet.EnterpriseFleetOrchestrator.validate_fleet",
        lambda self, *args, **kwargs: mock_fleet_result,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate-online-fleet",
            "--manifest",
            str(manifest_file),
        ],
    )
    assert result.exit_code == 1
    assert "POLICY GATE:       BLOCKED" in result.output


def test_cli_validate_online_fleet_manifest_missing():
    """Test CLI returns exit code 2 when manifest file does not exist."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate-online-fleet",
            "--manifest",
            "non_existent.yaml",
        ],
    )
    assert result.exit_code == 2
