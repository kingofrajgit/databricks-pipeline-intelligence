"""Integration tests for M5K Enterprise Fleet Online Validation."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from click.testing import CliRunner

from dpif.cli import cli
from dpif.connectors.offline import OfflineDatabricksConnector
from dpif.models import Checkpoint, CheckpointStatus, Severity
from dpif.models.fleet import (
    CollisionStatus,
    EnterpriseEnvironmentPolicy,
    EnterprisePipelineTarget,
    EnvironmentTier,
    FleetSummaryMetrics,
    FleetValidationResult,
    PipelineFleetExecution,
)
from dpif.models.rerun import RerunAnalysisResult
from dpif.models.synthesis import (
    DecisionRiskSynthesisResult,
)
from dpif.orchestration.fleet import EnterpriseFleetOrchestrator
from dpif.orchestration.online import OnlineValidationResult
from dpif.providers.base import DatabricksEvidenceProvider


class MockDatabricksConnector(OfflineDatabricksConnector):
    """Mock connector implementing DatabricksConnector for integration tests."""

    def __init__(self) -> None:
        super().__init__()

    def get_job(self, job_id: int | str) -> dict | None:
        return {"job_id": int(job_id), "settings": {"name": f"Job_{job_id}"}}

    def get_cluster(self, cluster_id: str) -> dict | None:
        return {"cluster_id": cluster_id, "spark_version": "13.3.x-scala2.12"}

    def get_table_profile(self, table: str) -> dict:
        return {"table_name": table, "row_count": 1000}

    def get_recent_runs(self, job_id: int | str, limit: int = 10) -> list[dict]:
        return []


def _create_mock_online_result(
    pid: str,
    target_table: str,
    write_mode: str,
    quality_score: float = 88.0,
    confidence: str = "HIGH",
    decision: str = "PRODUCTION_READY",
    has_blocking: bool = False,
) -> OnlineValidationResult:
    """Helper to assemble realistic OnlineValidationResult with M5E-M5I components."""
    rerun = MagicMock(spec=RerunAnalysisResult)
    rerun.target_table = target_table
    rerun.write_mode = write_mode

    synthesis = MagicMock(spec=DecisionRiskSynthesisResult)
    synthesis.quality_score = quality_score
    synthesis.production_blockers = ["Critical blocker"] if has_blocking else []
    synthesis.top_risks = [
        MagicMock(severity=Severity.CRITICAL if has_blocking else Severity.LOW, description="Risk A")
    ]
    synthesis.remediations = [
        MagicMock(priority="P0" if has_blocking else "P2", description="Remediation for " + pid, title="Remediation fix")
    ]

    cp1 = Checkpoint(
        checkpoint_id="CP-001",
        name="Source Profile",
        category="source",
        status=CheckpointStatus.PASS,
        score=90.0,
        severity=Severity.MEDIUM,
        findings=[],
    )
    cp18 = Checkpoint(
        checkpoint_id="CP-018",
        name="Rerun Safety",
        category="runtime",
        status=CheckpointStatus.PASS,
        score=90.0,
        severity=Severity.MEDIUM,
        findings=[],
    )

    return OnlineValidationResult(
        execution_mode="online",
        workspace="https://mock.cloud.databricks.com",
        resource_type="job",
        resource_id=pid,
        pipeline_name=f"Pipeline_{pid}",
        environment="production",
        quality_score=quality_score,
        confidence=confidence,
        decision_sufficiency=True,
        final_decision=decision,
        has_blocking=has_blocking,
        checkpoints={
            "CP-001": cp1,
            "CP-018": cp18,
        },
        findings=[],
        rerun_analysis=rerun,
        decision_risk_synthesis=synthesis,
    )


def test_online_fleet_multi_pipeline_end_to_end(tmp_path: Path):
    """End-to-end integration test: 3 pipelines validated with collision detection and policy gate."""
    manifest_file = tmp_path / "fleet.yaml"
    manifest_file.write_text(
        """
fleet:
  name: "enterprise-analytics-fleet"
  environment: "production"

workspace:
  host: "https://mock.cloud.databricks.com"

pipelines:
  - id: "p1_orders_append"
    job_id: 101
  - id: "p2_orders_overwrite"
    job_id: 102
  - id: "p3_kpi_summary"
    job_id: 103
""",
        encoding="utf-8",
    )

    mock_conn = MockDatabricksConnector()
    mock_prov = MagicMock(spec=DatabricksEvidenceProvider)

    orchestrator = EnterpriseFleetOrchestrator(connector=mock_conn, provider=mock_prov, max_workers=2)

    # Stub the internal _execute_single_target to produce distinct mock validation results
    def mock_target_exec(target, ws, policy, token=None):
        if target.id == "p1_orders_append":
            vr = _create_mock_online_result("p1_orders_append", "lakehouse.sales.orders", "append", 85.0)
        elif target.id == "p2_orders_overwrite":
            # p2 writes to the same table with overwrite! Confirmed collision
            vr = _create_mock_online_result("p2_orders_overwrite", "lakehouse.sales.orders", "overwrite", 82.0)
        else:
            vr = _create_mock_online_result("p3_kpi_summary", "lakehouse.analytics.kpis", "append", 94.0)

        passed, violations = policy.evaluate_pipeline(vr)
        return PipelineFleetExecution(
            pipeline_id=target.id,
            target=target,
            success=True,
            policy_passed=passed,
            policy_violations=violations,
            validation_result=vr,
            duration_seconds=0.15,
        )

    orchestrator._execute_single_target = mock_target_exec  # type: ignore[method-assign]

    from dpif.orchestration.fleet import load_and_validate_fleet_manifest

    manifest = load_and_validate_fleet_manifest(manifest_file)
    fleet_result = orchestrator.validate_fleet(manifest)

    # Assert fleet metrics
    assert fleet_result.fleet_name == "enterprise-analytics-fleet"
    assert fleet_result.summary.total_pipelines == 3
    assert fleet_result.summary.successful_validations == 3
    assert fleet_result.summary.failed_validations == 0

    # Collision detection
    assert len(fleet_result.collisions) == 1
    col = fleet_result.collisions[0]
    assert col.status == CollisionStatus.CONFIRMED
    assert col.severity == Severity.CRITICAL
    assert col.target_resource == "lakehouse.sales.orders"
    assert set(col.conflicting_pipeline_ids) == {"p1_orders_append", "p2_orders_overwrite"}

    # Policy gate must fail due to confirmed critical collision
    assert fleet_result.policy_passed is False


def test_online_fleet_failure_isolation_integration(tmp_path: Path):
    """Failure isolation integration test: Network error in pipeline B does not halt A or C."""
    manifest_file = tmp_path / "fleet_fail.yaml"
    manifest_file.write_text(
        """
fleet:
  name: "failure-isolation-fleet"
  environment: "staging"

pipelines:
  - id: "pA"
    job_id: 11
  - id: "pB_broken"
    job_id: 22
  - id: "pC"
    job_id: 33
""",
        encoding="utf-8",
    )

    orchestrator = EnterpriseFleetOrchestrator(max_workers=2)

    def mock_target_exec(target, ws, policy, token=None):
        if target.id == "pB_broken":
            # Simulate network failure
            return PipelineFleetExecution(
                pipeline_id=target.id,
                target=target,
                success=False,
                policy_passed=False,
                policy_violations=["DatabricksApiError: 503 Service Unavailable"],
                error_message="503 Service Unavailable",
                duration_seconds=0.05,
            )
        else:
            vr = _create_mock_online_result(target.id, f"table_{target.id}", "append", 90.0)
            passed, violations = policy.evaluate_pipeline(vr)
            return PipelineFleetExecution(
                pipeline_id=target.id,
                target=target,
                success=True,
                policy_passed=passed,
                policy_violations=violations,
                validation_result=vr,
                duration_seconds=0.1,
            )

    orchestrator._execute_single_target = mock_target_exec  # type: ignore[method-assign]

    from dpif.orchestration.fleet import load_and_validate_fleet_manifest

    manifest = load_and_validate_fleet_manifest(manifest_file)
    fleet_result = orchestrator.validate_fleet(manifest)

    assert fleet_result.summary.total_pipelines == 3
    assert fleet_result.summary.successful_validations == 2
    assert fleet_result.summary.failed_validations == 1
    assert fleet_result.pipeline_executions["pA"].success is True
    assert fleet_result.pipeline_executions["pB_broken"].success is False
    assert fleet_result.pipeline_executions["pC"].success is True
    assert fleet_result.policy_passed is False


def test_online_fleet_cli_with_exporters(tmp_path: Path, monkeypatch):
    """Integration test: CLI execution with JUnit, SARIF, and Markdown exports on disk."""
    manifest_file = tmp_path / "fleet_cli.yaml"
    manifest_file.write_text(
        """
fleet:
  name: "export-fleet"
  environment: "development"

pipelines:
  - id: "p1"
    job_id: 501
""",
        encoding="utf-8",
    )

    junit_path = tmp_path / "reports" / "junit.xml"
    sarif_path = tmp_path / "reports" / "sarif.json"
    md_path = tmp_path / "reports" / "summary.md"

    # Mock validate_fleet
    mock_vr = _create_mock_online_result("p1", "db.tbl1", "append", 95.0)

    mock_exec = PipelineFleetExecution(
        pipeline_id="p1",
        target=EnterprisePipelineTarget(id="p1", job_id=501),
        success=True,
        policy_passed=True,
        validation_result=mock_vr,
        duration_seconds=1.0,
    )

    mock_fleet_result = FleetValidationResult(
        fleet_name="export-fleet",
        environment=EnvironmentTier.DEVELOPMENT,
        policy=EnterpriseEnvironmentPolicy.default_for_tier(EnvironmentTier.DEVELOPMENT),
        summary=FleetSummaryMetrics(
            total_pipelines=1,
            successful_validations=1,
            passed_policy=1,
            fleet_quality_score=95.0,
        ),
        pipeline_executions={"p1": mock_exec},
        policy_passed=True,
        duration_seconds=1.2,
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
            "--export-junit",
            str(junit_path),
            "--export-sarif",
            str(sarif_path),
            "--export-markdown",
            str(md_path),
        ],
    )

    assert result.exit_code == 0
    assert "DPIF ENTERPRISE FLEET VALIDATION" in result.output
    assert "POLICY GATE:       PASSED" in result.output

    # Verify export files exist and are non-empty
    assert junit_path.exists()
    assert sarif_path.exists()
    assert md_path.exists()

    junit_content = junit_path.read_text(encoding="utf-8")
    assert "<testsuites" in junit_content
    assert "p1" in junit_content

    sarif_content = json.loads(sarif_path.read_text(encoding="utf-8"))
    assert sarif_content["version"] == "2.1.0"

    md_content = md_path.read_text(encoding="utf-8")
    assert "# ✅ DPIF Enterprise Fleet Validation Report" in md_content
