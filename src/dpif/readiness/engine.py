"""DPIF CP-FINAL Production Readiness Decision Engine.

Consolidates all preceding intelligence phases (Source, Data, Code, SQL,
Environment, Performance, Scalability) into a single, deterministic,
explainable, and machine-readable production release decision.

Strictly decouples:
1. Quality Score (0 - 100)
2. Evidence Coverage (0 - 100%)
3. Production Readiness (Decision Status)
"""

from __future__ import annotations

from typing import Any

from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    Finding,
    PipelineContract,
    Severity,
)
from dpif.readiness.coverage import compute_comprehensive_coverage
from dpif.readiness.models import (
    PrioritizedAction,
    ProductionReadinessAssessment,
    ProductionReadinessStatus,
    ReadinessPolicy,
)
from dpif.readiness.synthesizer import (
    compare_expected_implemented_actual,
    synthesize_cross_domain_risks,
)
from dpif.scoring.engine import score_checkpoints


def _assign_priority(finding: Finding) -> str:
    """Assign deterministic remediation priority to a finding."""
    if finding.blocking or finding.severity == Severity.CRITICAL:
        return "P0"
    if finding.severity == Severity.HIGH:
        return "P1"
    if finding.severity == Severity.MEDIUM:
        return "P2"
    return "P3"


def evaluate_production_readiness(
    checkpoints: dict[str, Checkpoint],
    contract: PipelineContract | None = None,
    profile: DataProfile | None = None,
    cluster_config: dict[str, Any] | None = None,
    job_config: dict[str, Any] | None = None,
    runtime_data: Any | None = None,
    historical_runs: list[Any] | None = None,
    actual_environment: dict[str, Any] | None = None,
    policy: ReadinessPolicy | None = None,
    connector_mode: str = "offline",
) -> ProductionReadinessAssessment:
    """Evaluate full pipeline readiness using deterministic precedence."""
    policy = policy or ReadinessPolicy()

    # 1. Compute Quality Score and Evidence Coverage
    score_obj, has_blocking = score_checkpoints(checkpoints)
    quality_score = score_obj.overall
    comprehensive_coverage = compute_comprehensive_coverage(checkpoints)
    coverage_pct = comprehensive_coverage.coverage_percentage

    # 2. Extract and classify all findings
    all_findings: list[Finding] = []
    for cp in checkpoints.values():
        all_findings.extend(cp.findings)

    blocking_findings = [
        f for f in all_findings if f.blocking and f.status == CheckpointStatus.FAIL
    ]
    critical_findings = [f for f in all_findings if f.severity == Severity.CRITICAL]
    high_findings = [
        f
        for f in all_findings
        if f.severity == Severity.HIGH and f.status == CheckpointStatus.FAIL
    ]
    warning_findings = [
        f
        for f in all_findings
        if f.status == CheckpointStatus.WARN or f.severity in (Severity.MEDIUM, Severity.LOW)
    ]

    warning_count = len(warning_findings)
    unknown_count = sum(1 for cp in checkpoints.values() if cp.status == CheckpointStatus.UNKNOWN)

    passed_cps = sum(1 for cp in checkpoints.values() if cp.status == CheckpointStatus.PASS)
    failed_cps = sum(1 for cp in checkpoints.values() if cp.status == CheckpointStatus.FAIL)
    unknown_cps = sum(1 for cp in checkpoints.values() if cp.status == CheckpointStatus.UNKNOWN)

    # 3. Synthesize Cross-Domain Risks & Configuration Drift
    cross_risks = synthesize_cross_domain_risks(
        checkpoints, contract, profile, cluster_config, runtime_data
    )
    config_diffs = compare_expected_implemented_actual(
        contract, cluster_config, job_config, profile, runtime_data, actual_environment
    )

    domain_assessments: dict[str, str] = {}
    for cp_id, cp in checkpoints.items():
        domain_assessments[f"{cp_id}_{cp.category}"] = cp.status.value

    # 4. Generate Decision Reasons & Action Items
    reasons: list[str] = []
    actions: list[PrioritizedAction] = []

    # Map findings to prioritized actions (deduplicated by rule_id)
    seen_rules: set[str] = set()
    for f in sorted(all_findings, key=lambda x: _assign_priority(x)):
        if f.rule_id in seen_rules:
            continue
        seen_rules.add(f.rule_id)
        prio = _assign_priority(f)
        actions.append(
            PrioritizedAction(
                priority=prio,
                rule_id=f.rule_id,
                checkpoint_id=getattr(f, "checkpoint_id", "") or f.category,
                title=f.title or f.name,
                description=(
                    f.description or (f.evidence.evidence[0] if f.evidence.evidence else "")
                ),
                recommendation=f.recommendation or f.evidence.recommendation,
            )
        )

    # Add cross-domain risks to actions if not already covered
    for xr in cross_risks:
        prio = "P0" if xr.severity == "CRITICAL" else "P1"
        actions.append(
            PrioritizedAction(
                priority=prio,
                rule_id=xr.risk_id,
                checkpoint_id="CROSS-DOMAIN",
                title=xr.title,
                description=xr.description,
                recommendation=xr.recommendation,
            )
        )

    # Sort actions deterministically: P0, P1, P2, P3
    actions.sort(key=lambda a: (a.priority, a.rule_id))

    # 5. Deterministic Precedence Evaluation
    # Check 1: Blocking Critical Finding
    blocking_crit = [f for f in blocking_findings if f.severity == Severity.CRITICAL]
    if blocking_crit:
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        items = [f"[{f.rule_id}] {f.title or f.name}" for f in blocking_crit]
        reasons.append(
            f"Release blocked by {len(blocking_crit)} CRITICAL finding(s): {', '.join(items)}"
        )

    # Check 2: Other Blocking Findings
    elif blocking_findings:
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        items = [f"[{f.rule_id}] {f.title or f.name}" for f in blocking_findings]
        reasons.append(
            f"Release blocked by {len(blocking_findings)} blocking finding(s): {', '.join(items)}"
        )

    # Check 3: Configured Policy Gates (Security, SLA, Reliability, High Severity)
    elif policy.block_on_critical and critical_findings:
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        items = [f"[{f.rule_id}] {f.title or f.name}" for f in critical_findings]
        reasons.append(
            f"Policy blocks release on CRITICAL findings ({len(critical_findings)} present): "
            f"{', '.join(items)}"
        )

    elif policy.block_on_high and high_findings:
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        items = [f"[{f.rule_id}] {f.title or f.name}" for f in high_findings]
        reasons.append(
            f"Policy blocks release on HIGH severity findings ({len(high_findings)} present): "
            f"{', '.join(items)}"
        )

    elif policy.block_on_security_failure and (
        checkpoints.get("CP-020") and checkpoints["CP-020"].status == CheckpointStatus.FAIL
    ):
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        reasons.append("Security validation (CP-020) failed blocking security gate.")

    elif policy.block_on_sla_failure and (
        checkpoints.get("CP-023") and checkpoints["CP-023"].status == CheckpointStatus.FAIL
    ):
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        reasons.append("SLA validation (CP-023) failed: execution exceeded contractual limit.")

    elif policy.block_on_reliability_failure and any(
        checkpoints.get(cp_id) and checkpoints[cp_id].status == CheckpointStatus.FAIL
        for cp_id in ("CP-014", "CP-015", "CP-016")
    ):
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        reasons.append("Reliability validation (CP-014..CP-016) failed blocking reliability gate.")

    elif any(d.drift_severity == "BLOCKING" for d in config_diffs):
        blocking_diffs = [d for d in config_diffs if d.drift_severity == "BLOCKING"]
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        for bd in blocking_diffs:
            reasons.append(f"Blocking configuration drift: {bd.notes or bd.parameter}")

    # Check 4: Missing Required Evidence (Evidence Gating)
    elif policy.require_runtime_evidence and (
        runtime_data is None
        or (checkpoints.get("CP-008") and checkpoints["CP-008"].status == CheckpointStatus.UNKNOWN)
    ):
        status = ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
        reasons.append(
            "Production policy requires runtime execution telemetry (CP-008), "
            "but no execution data was supplied."
        )

    elif policy.require_scalability_evidence and (
        not historical_runs
        or (checkpoints.get("CP-010") and checkpoints["CP-010"].status == CheckpointStatus.UNKNOWN)
    ):
        status = ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
        reasons.append(
            "Production policy requires scalability evidence (CP-010), "
            "but historical run data is missing."
        )

    elif policy.require_live_databricks_evidence and connector_mode not in ("live", "live-api"):
        status = ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
        reasons.append(
            "Production policy requires live Databricks environment verification, "
            "but run was offline."
        )

    elif coverage_pct < policy.minimum_evidence_coverage:
        status = ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
        reasons.append(
            f"Evidence coverage ({coverage_pct:.1f}%) is below the minimum required "
            f"threshold ({policy.minimum_evidence_coverage:.1f}%)."
        )

    # Check 5: Score Gating
    elif quality_score < policy.minimum_quality_score:
        status = ProductionReadinessStatus.NOT_PRODUCTION_READY
        reasons.append(
            f"Quality score ({quality_score:.1f}/100) is below the minimum threshold "
            f"({policy.minimum_quality_score:.1f}/100)."
        )

    # Check 6: Non-Blocking Warnings
    elif (
        warning_findings
        or any(cp.status == CheckpointStatus.WARN for cp in checkpoints.values())
        or cross_risks
    ):
        status = ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS
        reasons.append(
            f"No release-blocking failures detected, but {len(warning_findings)} warning(s) "
            "and operational advisories remain."
        )

    # Check 7: Clean Approval
    else:
        status = ProductionReadinessStatus.PRODUCTION_READY
        reasons.append(
            "All production gates satisfied with verified evidence coverage and no blocking risks."
        )

    # 6. Domain Summaries
    runtime_summary: dict[str, Any] = {}
    if runtime_data:
        runtime_summary = {
            "evidence_source": "FIXTURE" if connector_mode == "offline" else "RUNTIME",
            "duration_minutes": getattr(runtime_data, "total_duration_minutes", 0.0),
            "shuffle_gb": getattr(runtime_data, "total_shuffle_gb", 0.0),
            "spill_gb": getattr(runtime_data, "total_spill_gb", 0.0),
        }

    scalability_summary: dict[str, Any] = {
        "status": checkpoints.get("CP-010", Checkpoint(
            checkpoint_id="CP-010", name="Scalability", category="scalability",
            status=CheckpointStatus.UNKNOWN, severity=Severity.INFO, score=0.0
        )).status.value,
        "observations_count": len(historical_runs) if historical_runs else 0,
    }

    sla_summary: dict[str, Any] = {
        "status": checkpoints.get("CP-023", Checkpoint(
            checkpoint_id="CP-023", name="SLA", category="sla",
            status=CheckpointStatus.UNKNOWN, severity=Severity.INFO, score=0.0
        )).status.value,
        "max_runtime_minutes": (
            getattr(contract.sla, "max_runtime_minutes", None)
            if (contract and contract.sla)
            else None
        ),
    }

    security_summary: dict[str, Any] = {
        "status": checkpoints.get("CP-020", Checkpoint(
            checkpoint_id="CP-020", name="Security", category="security",
            status=CheckpointStatus.PASS, severity=Severity.INFO, score=1.0
        )).status.value,
    }

    reliability_summary: dict[str, Any] = {
        "retry_status": checkpoints.get("CP-014", Checkpoint(
            checkpoint_id="CP-014", name="Retry", category="reliability",
            status=CheckpointStatus.PASS, severity=Severity.INFO, score=1.0
        )).status.value,
        "restart_status": checkpoints.get("CP-015", Checkpoint(
            checkpoint_id="CP-015", name="Restart", category="reliability",
            status=CheckpointStatus.PASS, severity=Severity.INFO, score=1.0
        )).status.value,
        "idempotency_status": checkpoints.get("CP-016", Checkpoint(
            checkpoint_id="CP-016", name="Idempotency", category="reliability",
            status=CheckpointStatus.PASS, severity=Severity.INFO, score=1.0
        )).status.value,
    }

    return ProductionReadinessAssessment(
        status=status,
        quality_score=quality_score,
        evidence_coverage=coverage_pct,
        comprehensive_coverage=comprehensive_coverage,
        decision_reasons=reasons,
        required_actions=actions,
        blocking_findings=blocking_findings,
        critical_findings=critical_findings,
        high_findings=high_findings,
        warning_findings=warning_findings,
        warning_count=warning_count,
        unknown_count=unknown_count,
        applicable_checkpoint_count=len(checkpoints),
        passed_checkpoint_count=passed_cps,
        failed_checkpoint_count=failed_cps,
        unknown_checkpoint_count=unknown_cps,
        domain_assessments=domain_assessments,
        config_comparisons=config_diffs,
        cross_domain_risks=cross_risks,
        runtime_summary=runtime_summary,
        scalability_summary=scalability_summary,
        sla_summary=sla_summary,
        security_summary=security_summary,
        reliability_summary=reliability_summary,
        policy_version=policy.policy_version,
        connector_mode=connector_mode,
    )
