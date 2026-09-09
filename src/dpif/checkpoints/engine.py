from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from dpif.models import (
    AnalysisMethod,
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    EvidenceRecord,
    Finding,
    PipelineContract,
    Rule,
    Severity,
    Source,
)

logger = logging.getLogger(__name__)


class CheckpointEngine:
    """Engine that manages checkpoint execution, dependency resolution, and scoring."""

    def __init__(self) -> None:
        self.checkpoints: dict[str, Checkpoint] = {}
        self.evidence_by_checkpoint: dict[str, EvidenceRecord] = {}
        self.findings_by_checkpoint: dict[str, list[Finding]] = {}

    # -- registration ----------------------------------------------------
    def register_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Register a checkpoint with the engine."""
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint
        self.evidence_by_checkpoint[checkpoint.checkpoint_id] = checkpoint.evidence
        self.findings_by_checkpoint[checkpoint.checkpoint_id] = list(checkpoint.findings)
        logger.debug("Registered checkpoint: %s", checkpoint.checkpoint_id)

    def unregister_checkpoint(self, checkpoint_id: str) -> None:
        """Unregister a checkpoint."""
        self.checkpoints.pop(checkpoint_id, None)
        self.evidence_by_checkpoint.pop(checkpoint_id, None)
        self.findings_by_checkpoint.pop(checkpoint_id, None)
        logger.debug("Unregistered checkpoint: %s", checkpoint_id)

    def get_checkpoint(self, checkpoint_id: str) -> Checkpoint | None:
        """Get a checkpoint by ID."""
        return self.checkpoints.get(checkpoint_id)

    # -- execution -------------------------------------------------------
    def execute_checkpoint(
        self,
        checkpoint: Checkpoint,
        context: dict[str, Any],
    ) -> Checkpoint:
        """Execute a single checkpoint and update its status.

        Dependency rule (strict):
        - any dependency FAIL -> this checkpoint becomes UNKNOWN
        - any dependency UNKNOWN (or missing) -> UNKNOWN
        - never converts unavailable information into PASS
        """
        cp_id = checkpoint.checkpoint_id
        logger.info("Executing checkpoint: %s (%s)", cp_id, checkpoint.name)

        if cp_id == "CP-024":
            return self._execute_cp024_readiness(checkpoint, context)

        if checkpoint.depends_on:
            dep_result = self._check_dependencies(checkpoint, self.checkpoints)
            if dep_result is not True:
                reason = (
                    "dependency FAILED"
                    if dep_result is False
                    else "dependency evidence unavailable"
                )
                logger.warning("Dependency %s for checkpoint %s", reason, cp_id)
                checkpoint.status = CheckpointStatus.UNKNOWN
                checkpoint.severity = Severity.INFO
                checkpoint.assumptions = {
                    **checkpoint.assumptions,
                    "unknown-reason": f"Blocked: {reason}; result UNKNOWN (not PASS).",
                    "dependency": f"Blocked: {reason}; result UNKNOWN (not PASS).",
                }
                self._update_checkpoint(checkpoint, context)
                return checkpoint

        findings: list[Finding] = []
        applicable_rules = self._get_applicable_rules(checkpoint.category)
        logger.info("Found %d applicable rules for checkpoint %s", len(applicable_rules), cp_id)
        eval_context = self._evaluator_context(context)

        for rule in applicable_rules:
            try:
                rule_data = self._prepare_rule_data(checkpoint, context)
                finding = rule.evaluate(rule_data, eval_context)
                if finding is not None:
                    findings.append(finding)
                    logger.info(
                        "Rule %s produced %s for checkpoint %s",
                        rule.rule_id,
                        finding.status.value,
                        cp_id,
                    )
            except Exception as e:
                logger.error("Rule %s execution error: %s", rule.rule_id, e)
                findings.append(
                    Finding(
                        finding_id=f"{rule.rule_id}-error",
                        rule_id=rule.rule_id,
                        name=f"Rule evaluation error: {rule.name}",
                        title=f"Rule evaluation error: {rule.name}",
                        description=str(e),
                        category=rule.category,
                        status=CheckpointStatus.FAIL,
                        severity=Severity.HIGH,
                        pipeline_name=str(context.get("pipeline_name", "unknown")),
                        timestamp=datetime.now(UTC),
                        evidence=EvidenceRecord(
                            rule_id=rule.rule_id,
                            status=CheckpointStatus.FAIL,
                            severity=Severity.HIGH,
                            observed={"error": str(e)},
                            expected={"success": True},
                            evidence=[f"Rule execution failed: {e}"],
                            recommendation=f"Fix rule {rule.rule_id} evaluation",
                            confidence=1.0,
                        ),
                        recommendation=f"Fix rule {rule.rule_id} evaluation",
                        confidence=1.0,
                        blocking=False,
                        assumptions=dict(context.get("assumptions", {}) or {}),
                    )
                )

        if not findings:
            # No rule findings: preserve the builder's verdict (definitions
            # carry their own evidence). A code skeleton for actually-scanned,
            # clean code resolves to PASS; an explicitly UNKNOWN skeleton
            # (e.g. "no code available") stays UNKNOWN.
            if (
                checkpoint.category == "code"
                and checkpoint.status == CheckpointStatus.UNKNOWN
                and "unknown-reason" not in checkpoint.assumptions
            ):
                checkpoint.status = CheckpointStatus.PASS
                checkpoint.severity = Severity.INFO
                scan_data = self._prepare_rule_data(checkpoint, context)
                analysis = scan_data.get("code_analysis")
                counts = analysis.operation_counts() if analysis is not None else {}
                note = (
                    "No high-confidence static anti-patterns detected; "
                    "static analysis cannot prove runtime behavior or "
                    "complete optimization."
                )
                checkpoint.assumptions = {
                    **checkpoint.assumptions,
                    "operations_detected": counts,
                    "evidence_note": note,
                }
                checkpoint.evidence = EvidenceRecord(
                    rule_id=checkpoint.checkpoint_id,
                    status=CheckpointStatus.PASS,
                    severity=Severity.INFO,
                    observed={"operations_detected": counts},
                    expected={},
                    evidence=["AST static scan"],
                    recommendation=note,
                    confidence=0.7,
                    method=AnalysisMethod.METADATA,
                )
        elif any(
            f.severity == Severity.CRITICAL and f.status == CheckpointStatus.FAIL for f in findings
        ):
            checkpoint.status = CheckpointStatus.FAIL
            checkpoint.severity = Severity.CRITICAL
        elif any(f.status == CheckpointStatus.FAIL for f in findings):
            checkpoint.status = CheckpointStatus.FAIL
            checkpoint.severity = max(
                (f.severity for f in findings if f.status == CheckpointStatus.FAIL),
                key=lambda s: ["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"].index(s.value),
            )
        elif any(f.status == CheckpointStatus.WARN for f in findings):
            checkpoint.status = CheckpointStatus.WARN
            checkpoint.severity = Severity.MEDIUM
        else:
            checkpoint.status = CheckpointStatus.PASS
            checkpoint.severity = Severity.INFO

        checkpoint.findings = findings
        if findings:
            primary = findings[0]
            evidence_record = EvidenceRecord(
                rule_id=primary.rule_id,
                status=primary.status,
                severity=primary.severity,
                observed=primary.evidence.observed,
                expected=primary.evidence.expected,
                evidence=primary.evidence.evidence,
                recommendation=primary.evidence.recommendation,
                confidence=primary.evidence.confidence,
                method=primary.evidence.method,
            )
        else:
            # Preserve a meaningful pre-built evidence record (e.g. from the
            # checkpoint definitions) instead of wiping its reason.
            existing = checkpoint.evidence
            if existing.recommendation or existing.confidence or existing.observed:
                evidence_record = EvidenceRecord(
                    rule_id=existing.rule_id or checkpoint.checkpoint_id,
                    status=checkpoint.status,
                    severity=checkpoint.severity,
                    observed=existing.observed,
                    expected=existing.expected,
                    evidence=existing.evidence,
                    recommendation=existing.recommendation,
                    confidence=existing.confidence,
                    method=existing.method,
                )
            else:
                evidence_record = EvidenceRecord(
                    rule_id=checkpoint.checkpoint_id,
                    status=checkpoint.status,
                    severity=checkpoint.severity,
                    observed={},
                    expected={},
                    evidence=[],
                    recommendation="",
                    confidence=1.0 if checkpoint.status == CheckpointStatus.PASS else 0.0,
                    method=AnalysisMethod.UNAVAILABLE,
                )

        self.evidence_by_checkpoint[cp_id] = evidence_record
        self.findings_by_checkpoint[cp_id] = findings
        checkpoint.evidence = evidence_record
        checkpoint.timestamp = datetime.now(UTC)

        logger.info(
            "Checkpoint %s complete: status=%s, findings=%d",
            cp_id,
            checkpoint.status.value,
            len(findings),
        )
        return checkpoint

    def _execute_cp024_readiness(
        self,
        checkpoint: Checkpoint,
        context: dict[str, Any],
    ) -> Checkpoint:
        """Consolidate all evaluated checkpoints into CP-024 / CP-FINAL Production Readiness."""
        from dpif.readiness.engine import evaluate_production_readiness
        from dpif.readiness.models import ProductionReadinessStatus

        contract = context.get("pipeline_contract")
        profile = context.get("data_profile")
        cluster = context.get("cluster_config")
        job_cfg = context.get("job_config")
        run = context.get("runtime_run") or context.get("runtime_data")
        hist_runs = context.get("historical_runs")
        act_env = context.get("actual_environment")
        policy = context.get("readiness_policy")

        assessment = evaluate_production_readiness(
            checkpoints=self.checkpoints,
            contract=contract,
            profile=profile,
            cluster_config=cluster,
            job_config=job_cfg,
            runtime_data=run,
            historical_runs=hist_runs,
            actual_environment=act_env,
            policy=policy,
            connector_mode=context.get("connector_mode", "offline"),
        )
        context["production_readiness_assessment"] = assessment

        if assessment.status == ProductionReadinessStatus.PRODUCTION_READY:
            checkpoint.status = CheckpointStatus.PASS
            checkpoint.severity = Severity.INFO
        elif assessment.status == ProductionReadinessStatus.PRODUCTION_READY_WITH_WARNINGS:
            checkpoint.status = CheckpointStatus.WARN
            checkpoint.severity = Severity.MEDIUM
        elif assessment.status == ProductionReadinessStatus.NOT_PRODUCTION_READY:
            checkpoint.status = CheckpointStatus.FAIL
            checkpoint.severity = (
                Severity.CRITICAL
                if assessment.critical_findings or assessment.blocking_findings
                else Severity.HIGH
            )
        else:  # INSUFFICIENT_EVIDENCE
            checkpoint.status = CheckpointStatus.UNKNOWN
            checkpoint.severity = Severity.INFO

        checkpoint.score = assessment.quality_score / 100.0

        cp_findings: list[Finding] = []
        for i, reason in enumerate(assessment.decision_reasons):
            cp_findings.append(
                Finding(
                    finding_id=f"READINESS-DECISION-{i+1:03d}",
                    rule_id="CP-024-DECISION",
                    name=f"Readiness Decision Reason {i+1}",
                    title=reason,
                    description=reason,
                    category="readiness",
                    status=checkpoint.status,
                    severity=checkpoint.severity,
                    pipeline_name=str(context.get("pipeline_name", "unknown")),
                    timestamp=datetime.now(UTC),
                    evidence=EvidenceRecord(
                        rule_id="CP-024",
                        status=checkpoint.status,
                        severity=checkpoint.severity,
                        observed={
                            "status": assessment.status.value,
                            "quality_score": assessment.quality_score,
                        },
                        expected={"production_ready": True},
                        evidence=assessment.decision_reasons,
                        recommendation=(
                            assessment.required_actions[0].recommendation
                            if assessment.required_actions
                            else ""
                        ),
                        confidence=1.0,
                        method=AnalysisMethod.METADATA,
                    ),
                    recommendation=(
                        assessment.required_actions[0].recommendation
                        if assessment.required_actions
                        else ""
                    ),
                    confidence=1.0,
                    blocking=(checkpoint.status == CheckpointStatus.FAIL),
                )
            )

        evidence_record = EvidenceRecord(
            rule_id="CP-024",
            status=checkpoint.status,
            severity=checkpoint.severity,
            observed={
                "status": assessment.status.value,
                "quality_score": assessment.quality_score,
                "evidence_coverage": assessment.evidence_coverage,
                "blocking_findings": len(assessment.blocking_findings),
                "warnings": assessment.warning_count,
                "unknowns": assessment.unknown_count,
            },
            expected={"production_ready": True},
            evidence=assessment.decision_reasons,
            recommendation="; ".join(a.recommendation for a in assessment.required_actions[:3]),
            confidence=1.0,
            method=AnalysisMethod.METADATA,
        )

        self.evidence_by_checkpoint[checkpoint.checkpoint_id] = evidence_record
        self.findings_by_checkpoint[checkpoint.checkpoint_id] = cp_findings
        checkpoint.evidence = evidence_record
        checkpoint.findings = cp_findings
        checkpoint.timestamp = datetime.now(UTC)
        self._update_checkpoint(checkpoint, context)

        logger.info(
            "Checkpoint CP-024 complete: status=%s, findings=%d",
            checkpoint.status.value,
            len(cp_findings),
        )
        return checkpoint

    def _check_dependencies(
        self,
        checkpoint: Checkpoint,
        checkpoints: dict[str, Checkpoint],
    ) -> bool | None:
        """Check checkpoint dependencies.

        Returns:
            True: all dependencies PASS (WARN is tolerated)
            False: at least one dependency FAIL
            None: missing/UNKNOWN dependency (insufficient evidence)
        """
        for dep_id in checkpoint.depends_on:
            if dep_id not in checkpoints:
                logger.warning("Dependency checkpoint not found: %s", dep_id)
                return None
            dep = checkpoints[dep_id]
            if dep.status == CheckpointStatus.FAIL:
                return False
            if dep.status == CheckpointStatus.UNKNOWN:
                return None
        return True

    def _evaluator_context(self, context: dict[str, Any]) -> dict[str, Any]:
        """Merge caller rule context with configured analyzer thresholds.

        Source/contract metadata (format, volumes, schemas, jdbc/streaming)
        is derived here so evaluators work regardless of which caller keys
        were supplied; explicit ``rule_context`` entries win.
        """
        from dpif.config import format_size_factors, get_settings

        settings = get_settings()
        merged: dict[str, Any] = {
            "thresholds": {
                "small_file_threshold_kb": settings.small_file_threshold_kb,
                "small_file_min_files": settings.small_file_min_files,
                "excessive_file_count": settings.excessive_file_count,
                "partition_imbalance_ratio": settings.partition_imbalance_ratio,
                "partition_imbalance_min_gb": settings.partition_imbalance_min_gb,
                "large_volume_gb": settings.large_volume_gb,
                "jdbc_parallel_min_gb": settings.jdbc_parallel_min_gb,
                "format_factors": format_size_factors(settings),
            }
        }

        def _first(*values: Any) -> Any:
            for value in values:
                if value is not None and value != "":
                    return value
            return None

        source = context.get("source")
        contract = context.get("pipeline_contract")
        if source is not None:
            fmt = getattr(source.format, "value", str(source.format))
            mode = getattr(source.ingestion_mode, "value", str(source.ingestion_mode))
            merged.setdefault("source_format", fmt)
            merged.setdefault("ingestion_mode", mode)
            merged["expected_volume_gb"] = _first(
                source.expected_volume_gb,
                getattr(contract, "expected_daily_volume_gb", None) or None,
            )
            merged["peak_volume_gb"] = _first(
                source.peak_volume_gb,
                getattr(contract, "peak_daily_volume_gb", None) or None,
            )
            merged.setdefault("partitioning", list(source.partitioning or []))
            if source.jdbc is not None:
                merged.setdefault("jdbc", source.jdbc.to_dict())
            if source.streaming is not None:
                merged.setdefault("streaming", source.streaming.to_dict())
            if source.schema_definition is not None:
                merged.setdefault("expected_schema", source.schema_definition.to_dict())
        elif contract is not None:
            merged["expected_volume_gb"] = contract.expected_daily_volume_gb or None
            merged["peak_volume_gb"] = contract.peak_daily_volume_gb or None
        profile = context.get("data_profile")
        if profile is not None:
            merged.setdefault("collection_method", profile.collection_method.value)
            merged.setdefault("evidence_source", profile.evidence_source or "profile metadata")
        merged.update(context.get("rule_context", {}) or {})
        return merged

    def _get_applicable_rules(self, category: str) -> list[Rule]:
        """Get rules applicable to a checkpoint category."""
        from dpif.rules.engine import load_rules

        try:
            all_rules = load_rules()
        except Exception as e:  # pragma: no cover - defensive
            logger.error("Could not load rules: %s", e)
            return []
        return [r for r in all_rules if r.category == category]

    def _prepare_rule_data(
        self,
        checkpoint: Checkpoint,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """Prepare data for rule evaluation based on checkpoint context."""
        category = checkpoint.category
        data: dict[str, Any] = {
            "pipeline_name": context.get("pipeline_name", "unknown"),
            "assumptions": context.get("assumptions", {}),
        }
        if context.get("rule_context"):
            data["context"] = context["rule_context"]

        if category == "source":
            source: Source | None = context.get("source")
            if source is not None:
                data["text"] = source.config.get("path", "") or source.path
                fmt = source.format.value if hasattr(source.format, "value") else str(source.format)
                mode = (
                    source.ingestion_mode.value
                    if hasattr(source.ingestion_mode, "value")
                    else str(source.ingestion_mode)
                )
                data["observed"] = {
                    "source_type": source.type.value,
                    "path": source.path,
                    "format": fmt,
                    "ingestion_mode": mode,
                    "expected_volume_gb": source.expected_volume_gb,
                    "peak_volume_gb": source.peak_volume_gb,
                    "partitioning": source.partitioning,
                    "jdbc": source.jdbc.to_dict() if source.jdbc else None,
                    "streaming": source.streaming.to_dict() if source.streaming else None,
                }
                if source.data_profile is not None:
                    data["observed"].update(source.data_profile.to_dict())
                    data["context"] = {
                        **data.get("context", {}),
                        "data_size_gb": source.data_profile.total_gb,
                        "source_type": source.type.value,
                    }
                if source.schema_definition is not None:
                    data["context"] = {
                        **data.get("context", {}),
                        "expected_schema": source.schema_definition.to_dict(),
                    }
            else:
                data["text"] = ""
                data["observed"] = {}
        elif category == "data":
            data_profile: DataProfile | None = context.get("data_profile")
            if data_profile is not None:
                data["text"] = str(data_profile.to_dict())
                data["observed"] = data_profile.to_dict()
                data["context"] = {
                    **data.get("context", {}),
                    "data_size_gb": data_profile.total_gb,
                }
            else:
                data["text"] = ""
                data["observed"] = {}
        elif category == "code":
            code_snippet: str = context.get("code_snippet", "")
            data["text"] = code_snippet
            data["observed"] = {"code_snippet": code_snippet[:2000]}
            data["source_file"] = context.get("code_filename", context.get("code_path", "<code>"))
            # Parse once per validation and share with every code evaluator.
            # The CLI pre-builds the analysis (proper filename); otherwise
            # parse here and cache on the context for the remaining rules.
            cached = context.get("code_analysis") or context.get("_code_analysis")
            if cached is None:
                from dpif.code.parser import analyze_source

                cached = analyze_source(code_snippet, filename=str(data["source_file"]))
                try:
                    context["_code_analysis"] = cached
                except TypeError:
                    pass  # read-only mapping: evaluators re-parse cheaply
            data["code_analysis"] = cached
            if hasattr(cached, "sql_analysis") and cached.sql_analysis is not None:
                data["sql_analysis"] = cached.sql_analysis
            elif "sql_analysis" in context:
                data["sql_analysis"] = context["sql_analysis"]
        elif category == "cluster":
            cluster_config: dict[str, Any] = context.get("cluster_config", {})
            data["text"] = str(cluster_config)
            data["observed"] = cluster_config
            data["cluster"] = context.get("cluster") or cluster_config
            data["cluster_config"] = cluster_config
            data["cluster_policy"] = context.get("cluster_policy")
            data["pipeline_contract"] = context.get("pipeline_contract")
        elif category == "job":
            job_config: dict[str, Any] = context.get("job_config", {})
            data["text"] = str(job_config)
            data["observed"] = job_config
            data["job"] = context.get("job") or job_config
            data["job_config"] = job_config
            data["pipeline_contract"] = context.get("pipeline_contract")
        elif category == "pipeline":
            pipeline_contract: PipelineContract | None = context.get("pipeline_contract")
            if pipeline_contract is not None:
                dumped = pipeline_contract.to_dict()
                data["text"] = str(dumped)
                data["observed"] = dumped
            else:
                data["text"] = ""
                data["observed"] = {}
        elif category in ("performance", "runtime"):
            runtime_run = context.get("runtime_run") or context.get("runtime_data")
            data["runtime_run"] = runtime_run
            data["runtime_data"] = runtime_run
            data["run"] = runtime_run
            data["text"] = str(runtime_run) if runtime_run else ""
            if runtime_run is not None and hasattr(runtime_run, "model_dump"):
                data["observed"] = runtime_run.model_dump()
            elif isinstance(runtime_run, dict):
                data["observed"] = runtime_run
            else:
                data["observed"] = {}
        elif category == "scalability":
            contract = context.get("pipeline_contract")
            profile = context.get("data_profile")
            cluster = context.get("cluster") or context.get("cluster_config")
            run = context.get("runtime_run") or context.get("runtime_data")
            code_an = context.get("code_analysis") or context.get("_code_analysis")
            if code_an is None and context.get("code_snippet"):
                from dpif.code.parser import analyze_source

                code_an = analyze_source(context.get("code_snippet", ""))
                context["_code_analysis"] = code_an

            data["pipeline_contract"] = contract
            data["data_profile"] = profile
            data["cluster"] = cluster
            data["cluster_config"] = context.get("cluster_config")
            data["runtime_run"] = run
            data["code_analysis"] = code_an
            data["sql_analysis"] = (
                getattr(code_an, "sql_analysis", None) if code_an else context.get("sql_analysis")
            )
            data["historical_runs"] = context.get("historical_runs", [])

            from dpif.scalability.engine import generate_scenarios

            b, e, p, g = generate_scenarios(contract, profile, run)
            data["scenarios"] = {
                "baseline": b.to_dict() if b else None,
                "expected": e.to_dict() if e else None,
                "peak": p.to_dict() if p else None,
                "growth": [s.to_dict() for s in g],
            }
            data["observed"] = data["scenarios"]
            data["text"] = str(data["scenarios"])
        else:
            data["text"] = ""
            data["observed"] = {}
        return data

    def _update_checkpoint(self, checkpoint: Checkpoint, context: dict[str, Any]) -> None:
        """Update checkpoint evidence and timestamp without running rules."""
        cp_id = checkpoint.checkpoint_id
        evidence_record = EvidenceRecord(
            rule_id=checkpoint.checkpoint_id,
            status=checkpoint.status,
            severity=checkpoint.severity,
            observed={},
            expected={},
            evidence=[],
            recommendation="",
            confidence=0.0,
            method=AnalysisMethod.UNAVAILABLE,
        )
        self.evidence_by_checkpoint[cp_id] = evidence_record
        self.findings_by_checkpoint[cp_id] = []
        checkpoint.evidence = evidence_record
        checkpoint.timestamp = datetime.now(UTC)

    def run_all_checkpoints(
        self,
        checkpoints: list[Checkpoint],
        context: dict[str, Any],
    ) -> dict[str, Checkpoint]:
        """Run all checkpoints in dependency order.

        A checkpoint becomes runnable once all its dependencies have been
        executed (whatever their outcome). FAIL/UNKNOWN dependencies cause
        the dependent to resolve to UNKNOWN inside ``execute_checkpoint``.
        """
        executed: dict[str, Checkpoint] = {}
        remaining = list(checkpoints)
        # Register everything up-front so dependency lookups work.
        for cp in remaining:
            if cp.checkpoint_id not in self.checkpoints:
                self.register_checkpoint(cp)
            else:
                self.checkpoints[cp.checkpoint_id] = cp

        progress = True
        while remaining and progress:
            progress = False
            for cp in list(remaining):
                active_deps = [dep for dep in (cp.depends_on or []) if dep in self.checkpoints]
                if all(dep in executed for dep in active_deps):
                    remaining.remove(cp)
                    executed[cp.checkpoint_id] = cp
                    self.execute_checkpoint(cp, context)
                    progress = True
        for cp in remaining:
            logger.warning("Could not order checkpoint %s; marking UNKNOWN", cp.checkpoint_id)
            cp.status = CheckpointStatus.UNKNOWN
            cp.assumptions = {
                **cp.assumptions,
                "dependency": "Unresolvable dependency order (possible cycle); UNKNOWN.",
            }
            self._update_checkpoint(cp, context)
            executed[cp.checkpoint_id] = cp
        return executed

    def get_score(self, checkpoint: Checkpoint) -> float:
        """Compute a score (0.0 - 1.0) for a checkpoint based on findings."""
        if checkpoint.status == CheckpointStatus.UNKNOWN:
            return 0.0
        if checkpoint.status == CheckpointStatus.NOT_APPLICABLE:
            return 1.0
        if not checkpoint.findings:
            return 1.0 if checkpoint.status == CheckpointStatus.PASS else 0.5
        total_weight = sum(f.evidence.confidence for f in checkpoint.findings)
        if total_weight == 0:
            return 1.0 if checkpoint.status == CheckpointStatus.PASS else 0.0
        failed_weight = sum(
            f.evidence.confidence for f in checkpoint.findings if f.status == CheckpointStatus.FAIL
        )
        warn_weight = sum(
            f.evidence.confidence for f in checkpoint.findings if f.status == CheckpointStatus.WARN
        )
        return max(0.0, 1.0 - (failed_weight / total_weight) - 0.5 * (warn_weight / total_weight))

    def get_overall_status(self, checkpoints: dict[str, Checkpoint]) -> str:
        """Get overall validation status from all checkpoints."""
        for cp in checkpoints.values():
            if cp.status == CheckpointStatus.FAIL and cp.severity == Severity.CRITICAL:
                return "CRITICAL"
            for f in cp.findings:
                if f.blocking and f.status == CheckpointStatus.FAIL:
                    return "CRITICAL"
            if cp.status == CheckpointStatus.FAIL:
                return "HIGH_RISK"
        if any(cp.status == CheckpointStatus.WARN for cp in checkpoints.values()):
            return "NEEDS_IMPROVEMENT"
        if any(cp.status == CheckpointStatus.UNKNOWN for cp in checkpoints.values()):
            return "UNKNOWN"
        passing = (CheckpointStatus.PASS, CheckpointStatus.NOT_APPLICABLE)
        if all(cp.status in passing for cp in checkpoints.values()):
            return "EXCELLENT"
        return "UNKNOWN"
