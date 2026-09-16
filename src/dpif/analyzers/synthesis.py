"""Decision & Risk Synthesis Analyzer (M5I).

Orchestrates and synthesizes existing intelligence from:
- 24 Checkpoints (CP-001..CP-024)
- M5E Developer Implementation Forensics
- M5F Rerun & Idempotency Forensics
- M5G Three-Layer Alignment
- M5H Evidence Coverage & Decision Sufficiency
- CP-FINAL Production Readiness Assessment

Produces an explainable, deterministic engineering decision.
"""

from __future__ import annotations

from typing import Any

from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    PipelineContract,
    Severity,
)
from dpif.models.sufficiency import ConfidenceLevel, EvidenceSufficiencyAssessment
from dpif.models.synthesis import (
    DecisionRiskSynthesisResult,
    DomainSynthesisSummary,
    FinalDecisionStatus,
    ProductionBlocker,
    RiskCategory,
    RiskChain,
    SynthesizedRemediation,
    SynthesizedRisk,
)
from dpif.readiness.models import (
    ProductionReadinessAssessment,
    ProductionReadinessStatus,
)


class DecisionRiskSynthesisAnalyzer:
    """Consolidates and synthesizes all intelligence phases into a final release decision."""

    def __init__(
        self,
        checkpoints: dict[str, Checkpoint],
        readiness: ProductionReadinessAssessment,
        implementation_forensics: Any | None = None,
        rerun_analysis: Any | None = None,
        alignment_analysis: Any | None = None,
        evidence_sufficiency: EvidenceSufficiencyAssessment | None = None,
        contract: PipelineContract | None = None,
        profile: DataProfile | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.checkpoints = checkpoints
        self.readiness = readiness
        self.implementation_forensics = implementation_forensics
        self.rerun_analysis = rerun_analysis
        self.alignment_analysis = alignment_analysis
        self.evidence_sufficiency = evidence_sufficiency
        self.contract = contract
        self.profile = profile
        self.context = context or {}

    def analyze(self) -> DecisionRiskSynthesisResult:
        """Run complete decision and risk synthesis."""
        # 1. Synthesize Risks across all domains
        synthesized_risks = self._synthesize_risks()

        # 2. Identify Production Blockers
        blockers = self._identify_blockers(synthesized_risks)

        # 3. Build Deterministic Causal Risk Chains
        risk_chains = self._build_risk_chains(synthesized_risks)

        # 4. Synthesize Remediations
        remediations = self._synthesize_remediations(synthesized_risks, blockers)

        # 5. Build 16-Domain Synthesis Summaries
        domain_summaries = self._build_domain_summaries(synthesized_risks, blockers)

        # 6. Synthesize Missing Evidence
        missing_evidence = self._collect_missing_evidence()

        # 7. Evaluate Deterministic Decision Hierarchy
        final_decision, decision_explanations, score_override = self._evaluate_decision_matrix(
            blockers, synthesized_risks
        )

        # 8. Overall Confidence & Decision Sufficiency from M5H
        overall_confidence = (
            self.evidence_sufficiency.overall_confidence
            if self.evidence_sufficiency
            else ConfidenceLevel.MEDIUM
        )
        overall_sufficiency = (
            self.evidence_sufficiency.overall_decision_sufficiency
            if self.evidence_sufficiency
            else True
        )

        # Prioritize top risks (sorted by severity and blocking status)
        severity_rank = {
            Severity.CRITICAL: 0,
            Severity.HIGH: 1,
            Severity.MEDIUM: 2,
            Severity.LOW: 3,
            Severity.INFO: 4,
        }
        top_risks = sorted(
            synthesized_risks,
            key=lambda r: (0 if r.blocking else 1, severity_rank.get(r.severity, 5), r.risk_id),
        )[:5]

        score_band = self._compute_score_band(self.readiness.quality_score)

        return DecisionRiskSynthesisResult(
            final_decision=final_decision,
            quality_score=self.readiness.quality_score,
            score_band=score_band,
            confidence=overall_confidence,
            decision_sufficiency=overall_sufficiency,
            decision_explanation=decision_explanations,
            score_override_reason=score_override,
            blockers=blockers,
            top_risks=top_risks,
            all_risks=synthesized_risks,
            risk_chains=risk_chains,
            missing_evidence=missing_evidence,
            remediations=remediations,
            domain_summaries=domain_summaries,
            metadata={
                "checkpoints_count": len(self.checkpoints),
                "blockers_count": len(blockers),
                "total_risks_count": len(synthesized_risks),
                "chains_count": len(risk_chains),
                "remediations_count": len(remediations),
            },
        )

    def _map_rule_to_category(self, rule_id: str, cp_category: str = "") -> RiskCategory:
        """Deterministically map a rule_id or category string to a RiskCategory."""
        rule_upper = rule_id.upper()
        if "SEC" in rule_upper or "SECURITY" in rule_upper:
            return RiskCategory.SECURITY
        if "GOV" in rule_upper or "GOVERNANCE" in rule_upper:
            return RiskCategory.GOVERNANCE
        if "SLA" in rule_upper:
            return RiskCategory.SLA
        if "COST" in rule_upper:
            return RiskCategory.COST
        if "RER-DUP" in rule_upper or "DUPLICATE" in rule_upper:
            return RiskCategory.DUPLICATE_DATA
        if "RER-IDM" in rule_upper or "IDEMPOTEN" in rule_upper:
            return RiskCategory.IDEMPOTENCY
        if "RER-CNC" in rule_upper or "CONCURREN" in rule_upper:
            return RiskCategory.CONCURRENCY
        if "RER-RET" in rule_upper or "RETRY" in rule_upper:
            return RiskCategory.RELIABILITY
        if "ALIGN" in rule_upper or "DRIFT" in rule_upper:
            return RiskCategory.CONFIGURATION_DRIFT
        if "SCALABILITY" in rule_upper or "SCALE" in rule_upper:
            return RiskCategory.SCALABILITY
        if "PERF" in rule_upper or "PERFORMANCE" in rule_upper:
            return RiskCategory.PERFORMANCE
        if "DATA" in rule_upper or "SCHEMA" in rule_upper:
            return RiskCategory.DATA_QUALITY
        if "CODE" in rule_upper or "SQL" in rule_upper or "IMP-" in rule_upper or "AST" in rule_upper:
            return RiskCategory.IMPLEMENTATION
        if "SOURCE" in rule_upper:
            return RiskCategory.OPERATIONAL
        if "UNKNOWN" in rule_upper or "EVIDENCE" in rule_upper:
            return RiskCategory.EVIDENCE

        # Fallback to category string
        cat_upper = cp_category.upper()
        for member in RiskCategory:
            if member.value in cat_upper:
                return member
        return RiskCategory.OPERATIONAL

    def _synthesize_risks(self) -> list[SynthesizedRisk]:
        """Consolidate findings from Checkpoints, M5E, M5F, M5G, and CP-024 into SynthesizedRisk objects."""
        risks: list[SynthesizedRisk] = []
        seen_risk_ids: set[str] = set()

        # 1. Consolidate from Checkpoint findings
        for cp_id, cp in sorted(self.checkpoints.items()):
            if cp_id in ("CP-024", "CP-FINAL"):
                continue
            for f in cp.findings:
                if f.status in (CheckpointStatus.PASS, CheckpointStatus.NOT_APPLICABLE):
                    continue

                category = self._map_rule_to_category(f.rule_id, cp.category)
                risk_id = f"RISK-{f.rule_id}"
                if risk_id in seen_risk_ids:
                    continue
                seen_risk_ids.add(risk_id)

                consequence = self._derive_consequence(category, f.severity, f.title or f.name)
                conf = ConfidenceLevel.HIGH if f.confidence >= 0.8 else ConfidenceLevel.MEDIUM

                risks.append(
                    SynthesizedRisk(
                        risk_id=risk_id,
                        category=category,
                        severity=f.severity,
                        title=f.title or f.name,
                        description=f.description or (f.evidence.evidence[0] if f.evidence.evidence else ""),
                        source_findings=[f.rule_id],
                        affected_domains=[cp.category or cp_id],
                        blocking=f.blocking or f.severity == Severity.CRITICAL,
                        consequence=consequence,
                        confidence=conf,
                        evidence_provenance=[ev for ev in f.evidence.evidence[:2]],
                    )
                )

        # 2. Consolidate from M5F Rerun Analysis
        if self.rerun_analysis and hasattr(self.rerun_analysis, "findings"):
            for rf in self.rerun_analysis.findings:
                if rf.status == CheckpointStatus.PASS:
                    continue
                category = self._map_rule_to_category(rf.rule_id, "rerun_idempotency")
                risk_id = f"RISK-{rf.rule_id}"
                if risk_id in seen_risk_ids:
                    # Update existing with additional provenance
                    for r in risks:
                        if r.risk_id == risk_id and rf.rule_id not in r.source_findings:
                            r.source_findings.append(rf.rule_id)
                    continue
                seen_risk_ids.add(risk_id)

                is_blocking = (
                    rf.severity in (Severity.CRITICAL, Severity.HIGH)
                    and rf.rule_id in ("RER-DUP-001", "RER-RET-001", "RER-IDM-003", "RER-CNC-001")
                )
                consequence = self._derive_consequence(category, rf.severity, rf.title)

                risks.append(
                    SynthesizedRisk(
                        risk_id=risk_id,
                        category=category,
                        severity=rf.severity,
                        title=rf.title,
                        description=rf.description,
                        source_findings=[rf.rule_id],
                        affected_domains=["rerun_idempotency", "reliability"],
                        blocking=is_blocking,
                        consequence=consequence,
                        confidence=ConfidenceLevel.MEDIUM,
                        evidence_provenance=rf.evidence[:2],
                    )
                )

        # 3. Consolidate from M5G Three-Layer Alignment
        if self.alignment_analysis and hasattr(self.alignment_analysis, "dimensions"):
            for dim_name, dim_res in self.alignment_analysis.dimensions.items():
                drift_sev_val = (
                    dim_res.drift_severity.value
                    if hasattr(dim_res.drift_severity, "value")
                    else str(getattr(dim_res, "drift_severity", "NONE"))
                )
                has_drift = (
                    getattr(dim_res, "drift_detected", False)
                    or getattr(dim_res, "status", None) in (CheckpointStatus.WARN, CheckpointStatus.FAIL)
                    or drift_sev_val in ("WARN", "BLOCKING")
                )
                if not has_drift:
                    continue
                sev = Severity.CRITICAL if drift_sev_val == "BLOCKING" else Severity.MEDIUM
                risk_id = f"RISK-ALIGN-{str(dim_name).upper()}"
                if risk_id in seen_risk_ids:
                    continue
                seen_risk_ids.add(risk_id)

                dim_findings = [f.rule_id for f in getattr(dim_res, "findings", [])] or [f"ALIGN-{str(dim_name).upper()}"]
                exp_val = getattr(dim_res, "expected_summary", getattr(dim_res, "expected_value", ""))
                impl_val = getattr(dim_res, "implemented_summary", getattr(dim_res, "implemented_value", ""))
                act_val = getattr(dim_res, "actual_summary", getattr(dim_res, "actual_value", ""))
                desc = (
                    f"Configuration divergence across layers on {dim_name}: "
                    f"Expected='{exp_val}', Implemented='{impl_val}', Actual='{act_val}'."
                )
                consequence = "Runtime behavior and dependencies will diverge from declared pipeline contract."

                risks.append(
                    SynthesizedRisk(
                        risk_id=risk_id,
                        category=RiskCategory.CONFIGURATION_DRIFT,
                        severity=sev,
                        title=f"Configuration Drift: {dim_name}",
                        description=desc,
                        source_findings=dim_findings,
                        affected_domains=["three_layer_alignment", str(dim_name)],
                        blocking=(drift_sev_val == "BLOCKING"),
                        consequence=consequence,
                        confidence=ConfidenceLevel.HIGH,
                        evidence_provenance=[f"Expected: {exp_val}", f"Implemented: {impl_val}"],
                    )
                )

        # 4. Consolidate CrossDomainRisks from CP-024
        if self.readiness and hasattr(self.readiness, "cross_domain_risks"):
            for xr in self.readiness.cross_domain_risks:
                risk_id = f"RISK-{xr.risk_id}"
                if risk_id in seen_risk_ids:
                    continue
                seen_risk_ids.add(risk_id)

                sev = Severity(xr.severity) if hasattr(Severity, xr.severity) else Severity.HIGH
                cat = self._map_rule_to_category(xr.risk_id, xr.title)
                risks.append(
                    SynthesizedRisk(
                        risk_id=risk_id,
                        category=cat,
                        severity=sev,
                        title=xr.title,
                        description=xr.description,
                        source_findings=[xr.risk_id],
                        affected_domains=xr.contributing_domains,
                        blocking=(sev in (Severity.CRITICAL, Severity.HIGH)),
                        consequence=xr.recommendation,
                        confidence=ConfidenceLevel.MEDIUM,
                        evidence_provenance=xr.evidence_sources[:2],
                    )
                )

        # 5. Consolidate Evidence Insufficiency from M5H
        if self.evidence_sufficiency:
            for d in self.evidence_sufficiency.decisions:
                if not d.is_sufficient and d.decision_status == "UNKNOWN":
                    risk_id = f"RISK-SUFF-{d.decision_name}"
                    if risk_id in seen_risk_ids:
                        continue
                    seen_risk_ids.add(risk_id)
                    risks.append(
                        SynthesizedRisk(
                            risk_id=risk_id,
                            category=RiskCategory.EVIDENCE,
                            severity=Severity.MEDIUM,
                            title=f"Insufficient Evidence: {d.decision_name}",
                            description=d.rationale,
                            source_findings=[f"DECISION-{d.decision_name}"],
                            affected_domains=[d.domain],
                            blocking=False,
                            consequence="Pipeline behavior under production conditions cannot be certified safe.",
                            confidence=ConfidenceLevel.INSUFFICIENT,
                            evidence_provenance=d.missing_evidence[:2],
                        )
                    )

        return risks

    def _derive_consequence(self, category: RiskCategory, severity: Severity, title: str) -> str:
        """Derive an explainable real-world operational consequence for a risk."""
        if category == RiskCategory.DUPLICATE_DATA:
            return "Target dataset will accumulate duplicate records upon pipeline retries or reruns, corrupting analytical integrity."
        if category == RiskCategory.IDEMPOTENCY:
            return "Pipeline reruns are non-deterministic and can cause silent data duplication or data loss."
        if category == RiskCategory.CONCURRENCY:
            return "Concurrent executions targeting the same dataset will trigger write conflicts, file locks, or race conditions."
        if category == RiskCategory.CONFIGURATION_DRIFT:
            return "Runtime configuration differs from tested code, creating unpredictable failures in live Databricks clusters."
        if category == RiskCategory.SCALABILITY:
            return "Workload surges may cause heavy executor memory spill, severe performance degradation, or cluster out-of-memory failures."
        if category == RiskCategory.SECURITY:
            return "Pipeline exposes credentials, violates access boundaries, or violates cloud security compliance."
        if category == RiskCategory.SLA:
            return "Execution duration may exceed contractual SLA windows, delaying downstream dependent jobs."
        if category == RiskCategory.COST:
            return "Unoptimized resource sizing or continuous full scans will cause budget overrun."
        if category == RiskCategory.DATA_QUALITY:
            return "Malformed, drifting, or unpartitioned data may propagate downstream into gold analytical tables."
        if category == RiskCategory.EVIDENCE:
            return "Decision sufficiency is unverified; pipeline cannot be guaranteed safe for production."
        return f"Operational risk associated with {title}."

    def _identify_blockers(self, risks: list[SynthesizedRisk]) -> list[ProductionBlocker]:
        """Identify hard production blockers that prevent release approval."""
        blockers: list[ProductionBlocker] = []
        seen_blocker_ids: set[str] = set()

        # 1. Blockers from CP-024 (CP-FINAL)
        if self.readiness:
            for bf in getattr(self.readiness, "blocking_findings", []):
                bid = f"BLK-{bf.rule_id}"
                if bid not in seen_blocker_ids:
                    seen_blocker_ids.add(bid)
                    cat = self._map_rule_to_category(bf.rule_id, bf.category)
                    blockers.append(
                        ProductionBlocker(
                            blocker_id=bid,
                            title=bf.title or bf.name,
                            description=bf.description or (bf.evidence.evidence[0] if bf.evidence.evidence else ""),
                            source=bf.rule_id,
                            category=cat,
                            severity=bf.severity,
                            resolution_requirement=bf.recommendation or "Remediate blocking finding before production release.",
                        )
                    )

            # Critical findings in CP-024
            for cf in getattr(self.readiness, "critical_findings", []):
                bid = f"BLK-{cf.rule_id}"
                if bid not in seen_blocker_ids:
                    seen_blocker_ids.add(bid)
                    cat = self._map_rule_to_category(cf.rule_id, cf.category)
                    blockers.append(
                        ProductionBlocker(
                            blocker_id=bid,
                            title=cf.title or cf.name,
                            description=cf.description or "",
                            source=cf.rule_id,
                            category=cat,
                            severity=Severity.CRITICAL,
                            resolution_requirement=cf.recommendation or "Resolve critical vulnerability.",
                        )
                    )

        # 2. Blockers from M5G Blocking Configuration Drift
        if self.alignment_analysis and hasattr(self.alignment_analysis, "dimensions"):
            for dim_name, dim_res in self.alignment_analysis.dimensions.items():
                drift_sev_val = (
                    dim_res.drift_severity.value
                    if hasattr(dim_res.drift_severity, "value")
                    else str(getattr(dim_res, "drift_severity", "NONE"))
                )
                if drift_sev_val == "BLOCKING":
                    bid = f"BLK-ALIGN-{str(dim_name).upper()}"
                    if bid not in seen_blocker_ids:
                        seen_blocker_ids.add(bid)
                        exp_val = getattr(dim_res, "expected_summary", getattr(dim_res, "expected_value", ""))
                        impl_val = getattr(dim_res, "implemented_summary", getattr(dim_res, "implemented_value", ""))
                        act_val = getattr(dim_res, "actual_summary", getattr(dim_res, "actual_value", ""))
                        blockers.append(
                            ProductionBlocker(
                                blocker_id=bid,
                                title=f"Blocking Configuration Drift: {dim_name}",
                                description=f"Expected='{exp_val}' vs Implemented='{impl_val}' vs Actual='{act_val}'",
                                source=f"ALIGN-{str(dim_name).upper()}",
                                category=RiskCategory.CONFIGURATION_DRIFT,
                                severity=Severity.CRITICAL,
                                resolution_requirement=f"Align {dim_name} between pipeline contract, implementation, and Databricks runtime.",
                            )
                        )

        # 3. Blockers from Synthesized Risks marked blocking
        for r in risks:
            if r.blocking:
                bid = f"BLK-{r.risk_id}"
                if bid not in seen_blocker_ids:
                    seen_blocker_ids.add(bid)
                    blockers.append(
                        ProductionBlocker(
                            blocker_id=bid,
                            title=r.title,
                            description=r.description,
                            source="; ".join(r.source_findings),
                            category=r.category,
                            severity=r.severity,
                            resolution_requirement=f"Remediate {r.title}: {r.consequence}",
                        )
                    )

        # 4. Check explicit score policy blocker (only if CP-024 or configured policy defines it)
        score_blocker = self._check_score_policy_blocker()
        if score_blocker and score_blocker.blocker_id not in seen_blocker_ids:
            seen_blocker_ids.add(score_blocker.blocker_id)
            blockers.append(score_blocker)

        return blockers

    def _check_score_policy_blocker(self) -> ProductionBlocker | None:
        """Check if CP-024 or an explicit configured release policy defines quality score as blocking.

        A score below 80 MUST NOT automatically create a ProductionBlocker unless:
        1. CP-024 already defines that threshold as a blocking policy, OR
        2. An existing configurable release policy explicitly defines it.
        """
        if not self.readiness:
            return None

        score = self.readiness.quality_score

        # 1. Check CP-024 checkpoint findings for explicit blocking decision
        cp024 = self.checkpoints.get("CP-024")
        if cp024 and cp024.findings:
            for f in cp024.findings:
                if f.blocking and f.status == CheckpointStatus.FAIL:
                    desc_lower = (f.title or f.description or "").lower()
                    if "quality score" in desc_lower and "minimum threshold" in desc_lower:
                        return ProductionBlocker(
                            blocker_id="BLK-CP024-SCORE",
                            title=f.title or f"Quality Score Below Policy Threshold ({score:.1f}/100)",
                            description=f.description or f"Quality score ({score:.1f}/100) failed CP-024 release policy gate.",
                            source=f.rule_id,
                            category=RiskCategory.DATA_QUALITY,
                            severity=f.severity if f.severity else Severity.HIGH,
                            resolution_requirement=f.recommendation or "Resolve outstanding checkpoint findings to raise quality score above policy threshold.",
                        )

        # 2. Check CP-024 ProductionReadinessAssessment decision_reasons for explicit score gating failure
        reasons = getattr(self.readiness, "decision_reasons", [])
        for r in reasons:
            r_lower = r.lower()
            if "quality score" in r_lower and "minimum threshold" in r_lower and ("below" in r_lower or "failed" in r_lower):
                return ProductionBlocker(
                    blocker_id="BLK-CP024-SCORE",
                    title=f"Quality Score Below Policy Gate: {r}",
                    description=r,
                    source="CP-024-DECISION",
                    category=RiskCategory.DATA_QUALITY,
                    severity=Severity.HIGH,
                    resolution_requirement="Resolve outstanding checkpoint findings to satisfy CP-024 quality score policy gate.",
                )

        # 3. Check explicit configurable release policy in context
        policy = (
            self.context.get("readiness_policy")
            or self.context.get("policy")
            or getattr(self.readiness, "policy", None)
        )
        if policy is not None:
            min_score = getattr(policy, "minimum_quality_score", None)
            if min_score is not None and min_score > 0.0 and score < min_score:
                return ProductionBlocker(
                    blocker_id="BLK-CP024-SCORE",
                    title=f"Quality Score Below Policy Threshold ({score:.1f}/100 < {min_score:.1f}/100)",
                    description=f"Quality score ({score:.1f}/100) is below configured policy threshold ({min_score:.1f}/100).",
                    source="CP-024-SCORE",
                    category=RiskCategory.DATA_QUALITY,
                    severity=Severity.HIGH,
                    resolution_requirement=f"Resolve outstanding checkpoint findings to raise overall quality score above {min_score:.1f}.",
                )

        return None

    @staticmethod
    def _compute_score_band(score: float) -> str:
        """Compute score band aligned with DPIF scoring engine."""
        if score >= 90.0:
            return "EXCELLENT"
        if score >= 80.0:
            return "GOOD"
        if score >= 70.0:
            return "NEEDS_IMPROVEMENT"
        if score >= 50.0:
            return "HIGH_RISK"
        return "CRITICAL"

    def _build_risk_chains(self, risks: list[SynthesizedRisk]) -> list[RiskChain]:
        """Construct deterministic causal risk chains from observed evidence."""
        chains: list[RiskChain] = []

        # Chain 1: Append & Retry Idempotency Failure
        has_append_dup = any(r.category in (RiskCategory.DUPLICATE_DATA, RiskCategory.IDEMPOTENCY) for r in risks)
        dup_risk_status = (
            getattr(self.rerun_analysis.duplicate_risk, "status", getattr(self.rerun_analysis.duplicate_risk, "overall_status", CheckpointStatus.PASS))
            if (self.rerun_analysis and getattr(self.rerun_analysis, "duplicate_risk", None))
            else CheckpointStatus.PASS
        )
        if has_append_dup or dup_risk_status != CheckpointStatus.PASS:
            chains.append(
                RiskChain(
                    chain_id="CHAIN-001",
                    title="Append Write Mode + Job Retry Data Duplication Chain",
                    steps=[
                        "Pipeline write mode configured as APPEND without explicit transaction boundaries",
                        "Automated job retries or task re-attempts enabled in job cluster specification",
                        "Absence of target key uniqueness constraints or pre-write source deduplication",
                        "Transient failure triggers automatic batch re-execution",
                        "Duplicate records written to target table, silently inflating downstream aggregations",
                    ],
                    root_cause="Unprotected APPEND write mode coupled with automated job retry",
                    ultimate_impact="Data duplication and corruption of target reporting tables",
                    source_findings=["RER-DUP-001", "RER-RET-001"],
                )
            )

        # Chain 2: Workload Surge & Shuffle Scalability Chain
        has_scalability_risk = any(r.category == RiskCategory.SCALABILITY for r in risks)
        if has_scalability_risk:
            chains.append(
                RiskChain(
                    chain_id="CHAIN-002",
                    title="Workload Surge & Network Shuffle Spill Chain",
                    steps=[
                        "Contract defines 2x-5x peak ingestion volume burst",
                        "Source dataset lacks partitioning or query contains unkeyed wide transformation",
                        "Worker nodes execute massive all-to-all network shuffle under peak load",
                        "Executor heap memory exhausted, forcing intermediate partitions to disk spill",
                        "Pipeline experiences severe performance degradation or executor out-of-memory crashes",
                    ],
                    root_cause="Wide transformations on unpartitioned volume under peak workload bursts",
                    ultimate_impact="Task eviction, executor spill, and potential SLA violation",
                    source_findings=["SCALABILITY-002", "SCALABILITY-007"],
                )
            )

        # Chain 3: Configuration Drift Incompatibility Chain
        has_drift_risk = any(r.category == RiskCategory.CONFIGURATION_DRIFT for r in risks)
        if has_drift_risk:
            chains.append(
                RiskChain(
                    chain_id="CHAIN-003",
                    title="Three-Layer Configuration Drift Incompatibility Chain",
                    steps=[
                        "Contract establishes canonical runtime version and cluster sizing",
                        "Code implementation or active Databricks workspace environment deviates from contract",
                        "Incompatible Databricks Runtime (DBR) or driver/worker memory limits applied",
                        "Pipeline experiences runtime class-path divergence or unexpected cluster throttling",
                    ],
                    root_cause="Unsynchronized configuration changes across contract, code, and live workspace",
                    ultimate_impact="Deployment failure or operational drift in production",
                    source_findings=["ALIGN-COMPUTE_RUNTIME", "ALIGN-CLUSTER_TOPOLOGY"],
                )
            )

        # Chain 4: Offline Telemetry Insufficiency Chain
        missing_runtime = (
            self.checkpoints.get("CP-008") and self.checkpoints["CP-008"].status == CheckpointStatus.UNKNOWN
        )
        if missing_runtime:
            chains.append(
                RiskChain(
                    chain_id="CHAIN-004",
                    title="Offline Telemetry & SLA Uncertainty Chain",
                    steps=[
                        "Offline static code and contract validation performed without active Spark event logs",
                        "Observed runtime execution duration, task metrics, and memory spill remain UNKNOWN",
                        "SLA compliance and cost viability cannot be empirically certified",
                        "Production release must remain CONDITIONAL or INSUFFICIENT pending telemetry",
                    ],
                    root_cause="Absence of runtime execution event logs and Spark telemetry in offline mode",
                    ultimate_impact="Inability to certify SLA compliance and cost boundaries prior to release",
                    source_findings=["CP-008", "CP-023"],
                )
            )

        return chains

    def _synthesize_remediations(
        self, risks: list[SynthesizedRisk], blockers: list[ProductionBlocker]
    ) -> list[SynthesizedRemediation]:
        """Consolidate and prioritize actionable remediation recommendations."""
        remediations: list[SynthesizedRemediation] = []
        seen_titles: set[str] = set()

        # Priority P0: Blockers
        for b in blockers:
            if b.title in seen_titles:
                continue
            seen_titles.add(b.title)
            remediations.append(
                SynthesizedRemediation(
                    remediation_id=f"REM-{b.blocker_id}",
                    priority="P0",
                    severity=b.severity,
                    category=b.category,
                    title=f"Resolve Blocker: {b.title}",
                    description=b.description,
                    source_findings=[b.source],
                    required_evidence=[b.resolution_requirement],
                    expected_outcome="Removes production deployment blocker.",
                )
            )

        # Priority P1: High severity risks
        for r in risks:
            if r.severity == Severity.HIGH and r.title not in seen_titles:
                seen_titles.add(r.title)
                remediations.append(
                    SynthesizedRemediation(
                        remediation_id=f"REM-{r.risk_id}",
                        priority="P1",
                        severity=Severity.HIGH,
                        category=r.category,
                        title=f"Mitigate High Risk: {r.title}",
                        description=r.description,
                        source_findings=r.source_findings,
                        required_evidence=r.evidence_provenance or [r.consequence],
                        expected_outcome="Eliminates high operational or data integrity risk.",
                    )
                )

        # Priority P2: Medium severity risks / warnings
        for r in risks:
            if r.severity == Severity.MEDIUM and r.title not in seen_titles:
                seen_titles.add(r.title)
                remediations.append(
                    SynthesizedRemediation(
                        remediation_id=f"REM-{r.risk_id}",
                        priority="P2",
                        severity=Severity.MEDIUM,
                        category=r.category,
                        title=f"Address Warning: {r.title}",
                        description=r.description,
                        source_findings=r.source_findings,
                        required_evidence=r.evidence_provenance or [r.consequence],
                        expected_outcome="Improves pipeline stability and efficiency.",
                    )
                )

        # Priority P3: Advisory / Evidence telemetry
        if self.evidence_sufficiency and self.evidence_sufficiency.actionable_recommendations:
            for rec in self.evidence_sufficiency.actionable_recommendations[:3]:
                if rec in seen_titles:
                    continue
                seen_titles.add(rec)
                remediations.append(
                    SynthesizedRemediation(
                        remediation_id=f"REM-EVID-{len(remediations)+1}",
                        priority="P3",
                        severity=Severity.INFO,
                        category=RiskCategory.EVIDENCE,
                        title="Provide Telemetry Evidence",
                        description=rec,
                        source_findings=["M5H-SUFFICIENCY"],
                        required_evidence=[rec],
                        expected_outcome="Achieves full evidence coverage and decision sufficiency.",
                    )
                )

        return remediations

    def _build_domain_summaries(
        self, risks: list[SynthesizedRisk], blockers: list[ProductionBlocker]
    ) -> dict[str, DomainSynthesisSummary]:
        """Build consolidated summaries for all 16 pipeline domains."""
        domains = [
            "source", "data", "code", "sql", "cluster", "job", "pipeline",
            "runtime", "historical_runs", "scalability", "security", "governance",
            "cost", "implementation_forensics", "rerun_idempotency", "three_layer_alignment",
        ]
        summaries: dict[str, DomainSynthesisSummary] = {}

        for dom in domains:
            dom_risks = [r for r in risks if dom in r.affected_domains or dom in r.category.value.lower()]
            dom_blockers = [b for b in blockers if dom in b.category.value.lower() or dom in b.source.lower()]

            dom_cps = [
                cp for cp in self.checkpoints.values()
                if dom in getattr(cp, "category", "").lower()
                or (dom == "runtime" and cp.checkpoint_id == "CP-008")
                or (dom == "cost" and cp.checkpoint_id == "CP-019")
                or (dom == "sla" and cp.checkpoint_id == "CP-023")
                or (dom == "source" and cp.checkpoint_id == "CP-001")
                or (dom == "data" and cp.checkpoint_id in ("CP-002", "CP-003", "CP-007", "CP-022"))
                or (dom == "code" and cp.checkpoint_id == "CP-004")
                or (dom == "cluster" and cp.checkpoint_id == "CP-009")
                or (dom == "scalability" and cp.checkpoint_id == "CP-010")
                or (dom == "job" and cp.checkpoint_id == "CP-011")
                or (dom == "security" and cp.checkpoint_id == "CP-020")
                or (dom == "governance" and cp.checkpoint_id == "CP-021")
            ]
            has_unknown_cp = any(cp.status == CheckpointStatus.UNKNOWN for cp in dom_cps)

            # Determine domain status
            if dom_blockers:
                status = "FAIL"
                sev = Severity.CRITICAL
            elif any(r.severity == Severity.HIGH for r in dom_risks):
                status = "FAIL"
                sev = Severity.HIGH
            elif any(r.severity == Severity.MEDIUM for r in dom_risks):
                status = "WARN"
                sev = Severity.MEDIUM
            elif has_unknown_cp:
                status = "UNKNOWN"
                sev = Severity.INFO
            elif self.evidence_sufficiency and dom in self.evidence_sufficiency.domain_coverages:
                dc = self.evidence_sufficiency.domain_coverages[dom]
                if not dc.decision_sufficient:
                    status = "UNKNOWN"
                    sev = Severity.INFO
                else:
                    status = "PASS"
                    sev = Severity.INFO
            else:
                status = "PASS"
                sev = Severity.INFO

            conf = ConfidenceLevel.HIGH
            suff = True
            missing: list[str] = []
            if self.evidence_sufficiency and dom in self.evidence_sufficiency.domain_coverages:
                dc = self.evidence_sufficiency.domain_coverages[dom]
                conf = dc.confidence
                suff = dc.decision_sufficient
                missing = getattr(dc, "evidence_unavailable", getattr(dc, "missing_signals", []))

            actions = [r.consequence for r in dom_risks[:2]]

            summaries[dom] = DomainSynthesisSummary(
                domain=dom,
                status=status,
                severity=sev,
                confidence=conf,
                decision_sufficiency=suff,
                key_risks=[r.title for r in dom_risks[:3]],
                blockers=[b.title for b in dom_blockers],
                missing_evidence=missing[:3],
                recommended_actions=actions,
            )

        return summaries

    def _collect_missing_evidence(self) -> list[str]:
        """Consolidate critical missing evidence items across M5H and checkpoints."""
        missing: list[str] = []
        seen: set[str] = set()

        if self.evidence_sufficiency:
            for item in self.evidence_sufficiency.critical_missing_evidence:
                if item not in seen:
                    seen.add(item)
                    missing.append(item)

        for cp in self.checkpoints.values():
            if cp.status == CheckpointStatus.UNKNOWN:
                rec = cp.assumptions.get("unknown-reason") or cp.evidence.recommendation
                if rec and rec not in seen:
                    seen.add(rec)
                    missing.append(f"{cp.checkpoint_id}: {rec}")

        return missing

    def _evaluate_decision_matrix(
        self, blockers: list[ProductionBlocker], risks: list[SynthesizedRisk]
    ) -> tuple[FinalDecisionStatus, list[str], str | None]:
        """Evaluate deterministic release decision hierarchy and score override explanation."""
        reasons: list[str] = []
        score = self.readiness.quality_score
        score_override_reason: str | None = None

        # Step 1: Check for Hard Production Blockers
        if blockers:
            status = FinalDecisionStatus.NOT_PRODUCTION_READY
            for b in blockers[:3]:
                reasons.append(f"Blocked by [{b.severity.value}] {b.title} (Source: {b.source})")
            has_non_score_blocker = any(
                "SCORE" not in b.blocker_id and "SCORE" not in b.source for b in blockers
            )
            if score >= 80.0 and has_non_score_blocker:
                non_score_blockers = [
                    b for b in blockers if "SCORE" not in b.blocker_id and "SCORE" not in b.source
                ]
                score_override_reason = (
                    f"Quality score ({score:.1f}/100) meets minimum threshold but is overridden "
                    f"by {len(blockers)} hard production blocker(s): {', '.join(b.title for b in non_score_blockers[:2])}."
                )
            return status, reasons, score_override_reason

        # Step 2: Check CP-024 Production Readiness Assessment status
        if self.readiness.status == ProductionReadinessStatus.NOT_PRODUCTION_READY:
            status = FinalDecisionStatus.NOT_PRODUCTION_READY
            for r in self.readiness.decision_reasons[:3]:
                reasons.append(f"Production readiness gate failure: {r}")
            has_non_score_gate = any(
                "quality score" not in r.lower() for r in self.readiness.decision_reasons
            )
            if score >= 80.0 and has_non_score_gate:
                score_override_reason = (
                    f"Quality score ({score:.1f}/100) is overridden by CP-024 readiness gate failures."
                )
            return status, reasons, score_override_reason

        # Step 3: Check for Evidence Insufficiency
        cp_insufficient = self.readiness.status == ProductionReadinessStatus.INSUFFICIENT_EVIDENCE
        m5h_insufficient = (
            self.evidence_sufficiency is not None
            and not self.evidence_sufficiency.overall_decision_sufficiency
            and any(not d.is_sufficient and d.decision_status == "UNKNOWN" for d in self.evidence_sufficiency.decisions)
        )

        if cp_insufficient or m5h_insufficient:
            status = FinalDecisionStatus.INSUFFICIENT_EVIDENCE
            if cp_insufficient:
                reasons.extend(self.readiness.decision_reasons)
            if m5h_insufficient:
                reasons.append(
                    "M5H Decision Sufficiency is FALSE: critical decision telemetry (runtime, SLA, cost) is unavailable."
                )
            if score >= 80.0:
                score_override_reason = (
                    f"Quality score ({score:.1f}/100) cannot certify release due to insufficient evidence "
                    f"for critical operational decisions."
                )
            return status, reasons, score_override_reason

        # Step 4: Check for Unresolved HIGH-Severity Risks
        high_risks = [r for r in risks if r.severity == Severity.HIGH]
        if high_risks:
            status = FinalDecisionStatus.NOT_PRODUCTION_READY
            for hr in high_risks[:3]:
                reasons.append(f"Unresolved HIGH risk: {hr.title} ({hr.consequence})")
            if score >= 80.0:
                score_override_reason = (
                    f"Quality score ({score:.1f}/100) is overridden by unresolved high-severity risk(s): "
                    f"{', '.join(hr.title for hr in high_risks[:2])}."
                )
            return status, reasons, score_override_reason

        # Step 5: Check for Medium Risks / Warnings (Conditional Approval)
        med_risks = [r for r in risks if r.severity == Severity.MEDIUM]
        is_warn = (
            self.readiness.status == ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS
            or bool(med_risks)
        )
        if is_warn:
            status = FinalDecisionStatus.CONDITIONAL
            reasons.append("Pipeline approved conditionally subject to remediation of warnings:")
            for mr in med_risks[:3]:
                reasons.append(f"  - [{mr.severity.value}] {mr.title}")
            return status, reasons, None

        # Step 6: Full Production Ready
        status = FinalDecisionStatus.PRODUCTION_READY
        reasons.append("All checkpoint criteria, policy gates, and idempotency guarantees satisfied.")
        return status, reasons, None
