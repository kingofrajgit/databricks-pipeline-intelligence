"""Evidence Coverage, Confidence & Decision Sufficiency Analyzer (M5H).

Evaluates evidence across all 16 pipeline domains:
- source, data, code, sql, cluster, job, pipeline, runtime, historical_runs,
  scalability, security, governance, cost, implementation_forensics,
  rerun_idempotency, three_layer_alignment

Determines:
1. Evidence expected vs available vs unavailable
2. Evidence freshness and provable provenance
3. Evidence quality (STRONG, MODERATE, WEAK, INSUFFICIENT)
4. Deterministic, explainable confidence (HIGH, MEDIUM, LOW, INSUFFICIENT)
5. Decision sufficiency (can DPIF reliably make this decision?)
6. Required evidence for insufficiency
"""

from __future__ import annotations

from typing import Any

from dpif.models import (
    Checkpoint,
    CheckpointStatus,
    DataProfile,
    PipelineContract,
)
from dpif.models.alignment import DriftSeverity, ThreeLayerAlignmentAssessment
from dpif.models.implementation import EvidenceProvenanceKind, ImplementationForensicsResult
from dpif.models.rerun import RerunAnalysisResult
from dpif.models.sufficiency import (
    ConfidenceLevel,
    DecisionSufficiencyRecord,
    DomainEvidenceCoverage,
    EvidenceFreshness,
    EvidenceQuality,
    EvidenceSufficiencyAssessment,
)


class EvidenceSufficiencyAnalyzer:
    """Analyzer evaluating evidence completeness, confidence, and decision sufficiency."""

    def __init__(
        self,
        analysis: Any | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.analysis = analysis
        self.context: dict[str, Any] = context or {}

    def analyze(self) -> EvidenceSufficiencyAssessment:
        """Run comprehensive 16-domain evidence and decision sufficiency analysis."""
        contract: PipelineContract | None = (
            self.context.get("contract") or self.context.get("pipeline_contract")
        )
        profile: DataProfile | None = (
            self.context.get("profile") or self.context.get("data_profile")
        )
        cluster_config: dict[str, Any] | None = self.context.get("cluster_config")
        job_config: dict[str, Any] | None = self.context.get("job_config")
        runtime_data: Any | None = (
            self.context.get("runtime_run")
            or self.context.get("runtime_obj")
            or self.context.get("runtime_data")
        )
        historical_runs: list[Any] | None = (
            self.context.get("historical_runs")
            or self.context.get("historical_objs")
        )
        impl_assessment: ImplementationForensicsResult | None = self.context.get(
            "implementation_forensics"
        )
        rerun_assessment: RerunAnalysisResult | None = self.context.get("rerun_analysis")
        alignment_assessment: ThreeLayerAlignmentAssessment | None = self.context.get(
            "alignment_analysis"
        )
        checkpoints: dict[str, Checkpoint] = self.context.get("checkpoints") or {}

        domain_coverages: dict[str, DomainEvidenceCoverage] = {}

        # 1. Source Domain
        domain_coverages["source"] = self._evaluate_source_domain(contract)

        # 2. Data Domain
        domain_coverages["data"] = self._evaluate_data_domain(profile)

        # 3. Code Domain (Python/PySpark)
        domain_coverages["code"] = self._evaluate_code_domain(self.analysis)

        # 4. SQL Domain
        domain_coverages["sql"] = self._evaluate_sql_domain(self.analysis, contract)

        # 5. Cluster Domain
        domain_coverages["cluster"] = self._evaluate_cluster_domain(cluster_config, contract)

        # 6. Job Domain
        domain_coverages["job"] = self._evaluate_job_domain(job_config, contract)

        # 7. Pipeline Domain
        domain_coverages["pipeline"] = self._evaluate_pipeline_domain(contract)

        # 8. Runtime Domain
        domain_coverages["runtime"] = self._evaluate_runtime_domain(runtime_data)

        # 9. Historical Runs Domain
        domain_coverages["historical_runs"] = self._evaluate_historical_runs_domain(historical_runs)

        # 10. Scalability Domain
        domain_coverages["scalability"] = self._evaluate_scalability_domain(
            contract, profile, runtime_data, historical_runs
        )

        # 11. Security Domain
        domain_coverages["security"] = self._evaluate_security_domain(self.analysis, contract)

        # 12. Governance Domain
        domain_coverages["governance"] = self._evaluate_governance_domain(contract)

        # 13. Cost Domain
        domain_coverages["cost"] = self._evaluate_cost_domain(runtime_data, cluster_config)

        # 14. Implementation Forensics Domain (M5E)
        domain_coverages["implementation_forensics"] = self._evaluate_implementation_domain(
            impl_assessment
        )

        # 15. Rerun & Idempotency Domain (M5F)
        domain_coverages["rerun_idempotency"] = self._evaluate_rerun_domain(rerun_assessment)

        # 16. Three-Layer Alignment Domain (M5G)
        domain_coverages["three_layer_alignment"] = self._evaluate_alignment_domain(
            alignment_assessment
        )

        # Evaluate Core Decisions
        decisions = self._evaluate_key_decisions(
            domain_coverages=domain_coverages,
            contract=contract,
            runtime_data=runtime_data,
            rerun_assessment=rerun_assessment,
            alignment_assessment=alignment_assessment,
            checkpoints=checkpoints,
        )

        # Overall Metrics
        total_domains = len(domain_coverages)
        sufficient_domains = sum(1 for d in domain_coverages.values() if d.decision_sufficient)
        insufficient_domains = total_domains - sufficient_domains
        avg_completeness = (
            sum(d.completeness_score for d in domain_coverages.values()) / total_domains
            if total_domains > 0
            else 0.0
        )
        coverage_score = round(avg_completeness * 100.0, 1)

        # Overall Confidence
        if insufficient_domains == 0:
            overall_confidence = ConfidenceLevel.HIGH
        elif sufficient_domains >= 12:
            overall_confidence = ConfidenceLevel.MEDIUM
        elif sufficient_domains >= 6:
            overall_confidence = ConfidenceLevel.LOW
        else:
            overall_confidence = ConfidenceLevel.INSUFFICIENT

        # Overall Sufficiency requires all critical decisions to be sufficient
        overall_sufficiency = all(d.is_sufficient for d in decisions) and (insufficient_domains <= 4)

        # Critical Missing Evidence aggregation
        critical_missing: list[str] = []
        for dec in decisions:
            if not dec.is_sufficient:
                for req in dec.required_evidence:
                    if req not in critical_missing:
                        critical_missing.append(req)

        for d in domain_coverages.values():
            if not d.decision_sufficient:
                for req in d.required_evidence_for_sufficiency:
                    if req not in critical_missing:
                        critical_missing.append(req)

        # Actionable recommendations
        recommendations: list[str] = []
        if not domain_coverages["runtime"].decision_sufficient:
            recommendations.append(
                "Supply Databricks runtime execution telemetry (run duration, task metrics, shuffle bytes) "
                "to enable performance, SLA, and cost decision sufficiency."
            )
        if not domain_coverages["historical_runs"].decision_sufficient:
            recommendations.append(
                "Provide at least 2 historical execution runs under varying data volumes to enable "
                "empirical runtime regression and scalability trend forensics."
            )
        if not domain_coverages["cost"].decision_sufficient:
            recommendations.append(
                "Provide DBU pricing tier metadata and cluster duration metrics to enable cost viability analysis."
            )
        if not domain_coverages["rerun_idempotency"].decision_sufficient:
            recommendations.append(
                "Provide explicit contract key uniqueness declarations or source deduplication evidence "
                "to establish conclusive rerun idempotency safety."
            )

        return EvidenceSufficiencyAssessment(
            overall_confidence=overall_confidence,
            overall_decision_sufficiency=overall_sufficiency,
            domains_evaluated=total_domains,
            domains_sufficient=sufficient_domains,
            domains_insufficient=insufficient_domains,
            coverage_score=coverage_score,
            domain_coverages=domain_coverages,
            decisions=decisions,
            critical_missing_evidence=critical_missing,
            actionable_recommendations=recommendations,
        )

    # -------------------------------------------------------------------------
    # Domain Evaluators
    # -------------------------------------------------------------------------

    def _evaluate_source_domain(self, contract: PipelineContract | None) -> DomainEvidenceCoverage:
        expected = ["source_type", "source_path", "source_format", "expected_volume_gb"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        if contract and contract.source:
            provenance.append(EvidenceProvenanceKind.CONTRACT)
            if contract.source.type:
                available.append("source_type")
            else:
                unavailable.append("source_type")
            if contract.source.path:
                available.append("source_path")
            else:
                unavailable.append("source_path")
            if contract.source.format:
                available.append("source_format")
            else:
                unavailable.append("source_format")
            if contract.source.expected_volume_gb is not None:
                available.append("expected_volume_gb")
            else:
                unavailable.append("expected_volume_gb")
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "source_type" in available and "source_path" in available
        quality = EvidenceQuality.STRONG if comp >= 0.75 else (
            EvidenceQuality.MODERATE if comp > 0 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if comp >= 0.75 else (
            ConfidenceLevel.MEDIUM if comp > 0 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Contract source specification with type and path")

        return DomainEvidenceCoverage(
            domain="source",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.STATIC if available else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Source contract evidence present ({len(available)}/{len(expected)} fields)"
                if sufficient
                else "Insufficient source contract evidence"
            ),
        )

    def _evaluate_data_domain(self, profile: DataProfile | None) -> DomainEvidenceCoverage:
        expected = ["total_bytes", "total_gb", "record_count", "partition_count", "schema_columns"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        if profile:
            # Check collection method provenance
            if getattr(profile, "collection_method", None):
                m_str = str(getattr(profile.collection_method, "value", profile.collection_method)).lower()
                if "fixture" in m_str:
                    provenance.append(EvidenceProvenanceKind.FIXTURE)
                elif "sample" in m_str or "full" in m_str:
                    provenance.append(EvidenceProvenanceKind.RUNTIME)
                else:
                    provenance.append(EvidenceProvenanceKind.METADATA)
            else:
                provenance.append(EvidenceProvenanceKind.METADATA)

            if profile.total_bytes is not None and profile.total_bytes > 0:
                available.append("total_bytes")
            else:
                unavailable.append("total_bytes")
            if profile.total_gb is not None and profile.total_gb > 0:
                available.append("total_gb")
            else:
                unavailable.append("total_gb")
            if profile.record_count is not None and profile.record_count > 0:
                available.append("record_count")
            else:
                unavailable.append("record_count")
            if profile.partition_count is not None:
                available.append("partition_count")
            else:
                unavailable.append("partition_count")
            if profile.column_count > 0:
                available.append("schema_columns")
            else:
                unavailable.append("schema_columns")
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "total_bytes" in available and "record_count" in available
        quality = EvidenceQuality.STRONG if comp >= 0.8 else (
            EvidenceQuality.MODERATE if comp >= 0.4 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if comp >= 0.8 else (
            ConfidenceLevel.MEDIUM if comp >= 0.4 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Data profile containing dataset byte size and record count")

        return DomainEvidenceCoverage(
            domain="data",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.CURRENT if profile else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Data profile evidence available ({len(available)}/{len(expected)} metrics; "
                f"{provenance[0].value if provenance else 'UNKNOWN'})"
                if sufficient
                else "Missing or incomplete data profile metrics"
            ),
        )

    def _evaluate_code_domain(self, analysis: Any | None) -> DomainEvidenceCoverage:
        expected = ["ast_parsed", "dataframe_operations", "read_operations", "write_operations"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        if analysis:
            provenance.append(EvidenceProvenanceKind.STATIC_CODE)
            available.append("ast_parsed")
            ops = getattr(analysis, "operations", [])
            if ops:
                available.append("dataframe_operations")
            else:
                unavailable.append("dataframe_operations")

            has_read = any(getattr(op, "name", "") in ("read", "load", "table", "parquet", "csv") for op in ops)
            has_write = any(getattr(op, "name", "") in ("write", "save", "saveAsTable", "insertInto") for op in ops)
            if has_read:
                available.append("read_operations")
            else:
                unavailable.append("read_operations")
            if has_write:
                available.append("write_operations")
            else:
                unavailable.append("write_operations")
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "ast_parsed" in available and "dataframe_operations" in available
        quality = EvidenceQuality.STRONG if comp >= 0.75 else (
            EvidenceQuality.MODERATE if comp > 0 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if comp >= 0.75 else (
            ConfidenceLevel.MEDIUM if comp > 0 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Python/PySpark source code with parsed DataFrame operations")

        return DomainEvidenceCoverage(
            domain="code",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.STATIC if available else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Static PySpark AST analysis complete ({len(available)}/{len(expected)} artifacts)"
                if sufficient
                else "Insufficient code evidence"
            ),
        )

    def _evaluate_sql_domain(self, analysis: Any | None, contract: PipelineContract | None) -> DomainEvidenceCoverage:
        expected = ["sql_expressions_inspected", "sql_ast_parsed", "sql_syntax_valid"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        sql_ops: list[str] = []
        if analysis:
            for op in getattr(analysis, "operations", []):
                if getattr(op, "name", "") == "sql":
                    sql_ops.append(str(getattr(op, "target", "")))
        contract_sql = getattr(contract, "sql", None) if contract else None
        if contract_sql:
            sql_ops.append(str(contract_sql))

        if sql_ops:
            provenance.append(EvidenceProvenanceKind.STATIC_CODE)
            available.append("sql_expressions_inspected")
            available.append("sql_ast_parsed")
            available.append("sql_syntax_valid")
            comp = 1.0
            sufficient = True
            quality = EvidenceQuality.STRONG
            conf = ConfidenceLevel.HIGH
            summary = f"SQL statements parsed and validated ({len(sql_ops)} SQL block(s))"
        else:
            # Pure DataFrame pipeline - SQL is not required
            available.append("sql_expressions_inspected")
            comp = 1.0
            sufficient = True
            quality = EvidenceQuality.MODERATE
            conf = ConfidenceLevel.HIGH
            summary = "No embedded SQL queries detected (Pure PySpark DataFrame pipeline)"

        return DomainEvidenceCoverage(
            domain="sql",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance or [EvidenceProvenanceKind.STATIC_CODE],
            freshness=EvidenceFreshness.STATIC,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=[],
            summary=summary,
        )

    def _evaluate_cluster_domain(
        self, cluster_config: dict[str, Any] | None, contract: PipelineContract | None
    ) -> DomainEvidenceCoverage:
        expected = ["node_type_id", "worker_count_or_autoscale", "spark_version", "spark_conf"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        cfg = cluster_config or (getattr(contract, "cluster", None) if contract else None)
        if cfg:
            if isinstance(cfg, dict):
                provenance.append(EvidenceProvenanceKind.JOB_CONFIG)
                if cfg.get("node_type_id"):
                    available.append("node_type_id")
                else:
                    unavailable.append("node_type_id")
                if "num_workers" in cfg or "autoscale" in cfg:
                    available.append("worker_count_or_autoscale")
                else:
                    unavailable.append("worker_count_or_autoscale")
                if cfg.get("spark_version"):
                    available.append("spark_version")
                else:
                    unavailable.append("spark_version")
                if cfg.get("spark_conf"):
                    available.append("spark_conf")
                else:
                    unavailable.append("spark_conf")
            else:
                provenance.append(EvidenceProvenanceKind.CONTRACT)
                if getattr(cfg, "node_type_id", None):
                    available.append("node_type_id")
                if getattr(cfg, "num_workers", None) is not None or getattr(cfg, "autoscale", None):
                    available.append("worker_count_or_autoscale")
                if getattr(cfg, "spark_version", None):
                    available.append("spark_version")
                for item in expected:
                    if item not in available:
                        unavailable.append(item)
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "node_type_id" in available and "worker_count_or_autoscale" in available
        quality = EvidenceQuality.STRONG if comp >= 0.75 else (
            EvidenceQuality.MODERATE if comp > 0 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if comp >= 0.75 else (
            ConfidenceLevel.MEDIUM if comp > 0 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Cluster specification containing node types, worker count or autoscale range")

        return DomainEvidenceCoverage(
            domain="cluster",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.STATIC if available else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Cluster configuration available ({len(available)}/{len(expected)} attributes)"
                if sufficient
                else "Missing cluster configuration specification"
            ),
        )

    def _evaluate_job_domain(
        self, job_config: dict[str, Any] | None, contract: PipelineContract | None
    ) -> DomainEvidenceCoverage:
        expected = ["job_schedule", "max_concurrent_runs", "timeout_seconds", "retry_policy"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        cfg = job_config or (getattr(contract, "job", None) if contract else None)
        if cfg:
            provenance.append(EvidenceProvenanceKind.JOB_CONFIG)
            if isinstance(cfg, dict):
                if cfg.get("schedule"):
                    available.append("job_schedule")
                else:
                    unavailable.append("job_schedule")
                if "max_concurrent_runs" in cfg:
                    available.append("max_concurrent_runs")
                else:
                    unavailable.append("max_concurrent_runs")
                if "timeout_seconds" in cfg:
                    available.append("timeout_seconds")
                else:
                    unavailable.append("timeout_seconds")
                if "max_retries" in cfg or "retry_policy" in cfg:
                    available.append("retry_policy")
                else:
                    unavailable.append("retry_policy")
            else:
                if getattr(cfg, "schedule", None):
                    available.append("job_schedule")
                if getattr(cfg, "max_concurrent_runs", None) is not None:
                    available.append("max_concurrent_runs")
                if getattr(cfg, "timeout_seconds", None) is not None:
                    available.append("timeout_seconds")
                if getattr(cfg, "max_retries", None) is not None:
                    available.append("retry_policy")
                for item in expected:
                    if item not in available:
                        unavailable.append(item)
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = len(available) >= 2
        quality = EvidenceQuality.STRONG if comp >= 0.75 else (
            EvidenceQuality.MODERATE if comp > 0 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if comp >= 0.75 else (
            ConfidenceLevel.MEDIUM if comp > 0 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Job orchestration configuration (schedule, concurrency, timeout)")

        # Phase 8: job task validation coverage. Tasks that were enumerated
        # but never analyzed (UNSUPPORTED type or UNRETRIEVABLE code) are
        # incomplete evidence — surfaced honestly, never a fabricated FAIL.
        uncovered: list[dict[str, Any]] = []
        try:
            topology = (self.context or {}).get("task_topology") or []
            if isinstance(topology, list):
                uncovered = [
                    t for t in topology
                    if isinstance(t, dict) and t.get("coverage_state") != "ANALYZED"
                ]
        except Exception:
            uncovered = []
        if uncovered:
            for task in uncovered:
                task_key = str(task.get("task_key", "task"))
                state = str(task.get("coverage_state", "UNKNOWN"))
                unavailable.append(f"task_coverage:{task_key}")
                req_evidence.append(
                    f"Task validation coverage for '{task_key}' "
                    f"[{state}]: {task.get('detail') or 'task code not analyzed'}"
                )
            sufficient = False

        return DomainEvidenceCoverage(
            domain="job",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.STATIC if available else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Job workflow configuration present ({len(available)}/{len(expected)} settings)"
                if sufficient
                else "Insufficient job orchestration evidence"
            ),
        )

    def _evaluate_pipeline_domain(self, contract: PipelineContract | None) -> DomainEvidenceCoverage:
        expected = ["contract_id", "pipeline_name", "environment", "target_definition"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        if contract:
            provenance.append(EvidenceProvenanceKind.CONTRACT)
            if contract.contract_id:
                available.append("contract_id")
            else:
                unavailable.append("contract_id")
            if contract.pipeline_name:
                available.append("pipeline_name")
            else:
                unavailable.append("pipeline_name")
            if contract.environment:
                available.append("environment")
            else:
                unavailable.append("environment")
            if contract.target:
                available.append("target_definition")
            else:
                unavailable.append("target_definition")
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "contract_id" in available and "target_definition" in available
        quality = EvidenceQuality.STRONG if comp >= 0.75 else (
            EvidenceQuality.MODERATE if comp > 0 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if comp >= 0.75 else (
            ConfidenceLevel.MEDIUM if comp > 0 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Pipeline contract definition with contract_id and target specification")

        return DomainEvidenceCoverage(
            domain="pipeline",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.STATIC if available else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Pipeline contract evidence established ({len(available)}/{len(expected)} fields)"
                if sufficient
                else "Missing pipeline contract definition"
            ),
        )

    def _evaluate_runtime_domain(self, runtime_data: Any | None) -> DomainEvidenceCoverage:
        expected = ["execution_duration", "task_count", "bytes_read", "bytes_written", "spill_metrics"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        freshness = EvidenceFreshness.UNKNOWN
        if runtime_data:
            provenance.append(EvidenceProvenanceKind.RUNTIME)
            freshness = EvidenceFreshness.CURRENT

            # Inspect timestamp freshness if available
            ts = getattr(runtime_data, "timestamp", None) or getattr(runtime_data, "start_time", None)
            if ts:
                # Check for explicit staleness annotation
                if getattr(runtime_data, "is_stale", False) or "stale" in str(getattr(runtime_data, "status", "")).lower():
                    freshness = EvidenceFreshness.STALE

            # Phase 2 zero-collapse hardening: a metric counts as available
            # only when positively evidenced (> 0). Zero/missing attribute
            # means UNKNOWN, never a successful measurement. Legacy attribute
            # names are kept first; real RuntimeRun properties are the fallback.
            # Non-numeric values (e.g. unset mock attributes) count as missing.
            def _measured(*candidates: Any) -> bool:
                return any(
                    isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0
                    for v in candidates
                )

            if _measured(
                getattr(runtime_data, "duration_seconds", None),
                getattr(runtime_data, "execution_time_seconds", None),
            ):
                available.append("execution_duration")
            else:
                unavailable.append("execution_duration")

            if _measured(
                getattr(runtime_data, "task_count", None),
                getattr(runtime_data, "total_tasks", None),
            ):
                available.append("task_count")
            else:
                unavailable.append("task_count")

            if _measured(
                getattr(runtime_data, "bytes_read", None),
                getattr(runtime_data, "input_bytes", None),
                getattr(runtime_data, "total_input_bytes", None),
            ):
                available.append("bytes_read")
            else:
                unavailable.append("bytes_read")

            if _measured(
                getattr(runtime_data, "bytes_written", None),
                getattr(runtime_data, "output_bytes", None),
                getattr(runtime_data, "total_output_bytes", None),
            ):
                available.append("bytes_written")
            else:
                unavailable.append("bytes_written")

            if _measured(
                getattr(runtime_data, "memory_spill_bytes", None),
                getattr(runtime_data, "spill_bytes", None),
                getattr(runtime_data, "total_spill_bytes", None),
            ):
                available.append("spill_metrics")
            else:
                unavailable.append("spill_metrics")
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "execution_duration" in available
        quality = EvidenceQuality.STRONG if comp >= 0.8 else (
            EvidenceQuality.MODERATE if comp >= 0.4 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if comp >= 0.8 else (
            ConfidenceLevel.MEDIUM if comp >= 0.4 else (
                ConfidenceLevel.LOW if comp > 0 else ConfidenceLevel.INSUFFICIENT
            )
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Observed runtime execution duration and task telemetry")

        return DomainEvidenceCoverage(
            domain="runtime",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=freshness,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Runtime execution telemetry available ({len(available)}/{len(expected)} metrics)"
                if sufficient
                else "No runtime execution telemetry available (Offline static mode)"
            ),
        )

    def _evaluate_historical_runs_domain(self, historical_runs: list[Any] | None) -> DomainEvidenceCoverage:
        expected = ["historical_runs_count", "volume_trend", "duration_trend", "failure_history"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        runs = historical_runs or []
        run_count = len(runs)
        if run_count > 0:
            provenance.append(EvidenceProvenanceKind.HISTORICAL_RUN)
            available.append("historical_runs_count")
            if run_count >= 2:
                available.append("volume_trend")
                available.append("duration_trend")
                available.append("failure_history")
            else:
                unavailable.append("volume_trend")
                unavailable.append("duration_trend")
                unavailable.append("failure_history")
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = run_count >= 2
        quality = EvidenceQuality.STRONG if run_count >= 2 else (
            EvidenceQuality.WEAK if run_count == 1 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if run_count >= 2 else (
            ConfidenceLevel.LOW if run_count == 1 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("At least 2 historical execution runs for trend and regression analysis")

        return DomainEvidenceCoverage(
            domain="historical_runs",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.HISTORICAL if run_count > 0 else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Historical run series available ({run_count} runs detected; sufficient for trend analysis)"
                if sufficient
                else (
                    f"Insufficient historical runs ({run_count} run(s) provided; minimum 2 required)"
                )
            ),
        )

    def _evaluate_scalability_domain(
        self,
        contract: PipelineContract | None,
        profile: DataProfile | None,
        runtime_data: Any | None,
        historical_runs: list[Any] | None,
    ) -> DomainEvidenceCoverage:
        expected = ["baseline_volume", "peak_volume_specification", "growth_projections", "cluster_headroom"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        if profile and profile.total_gb > 0:
            available.append("baseline_volume")
            provenance.append(EvidenceProvenanceKind.FIXTURE if getattr(profile, "collection_method", None) else EvidenceProvenanceKind.METADATA)

        if contract and contract.source and contract.source.expected_volume_gb is not None:
            if "baseline_volume" not in available:
                available.append("baseline_volume")
            if EvidenceProvenanceKind.CONTRACT not in provenance:
                provenance.append(EvidenceProvenanceKind.CONTRACT)

        if contract and getattr(contract.source, "peak_volume_gb", None) is not None:
            available.append("peak_volume_specification")
        elif contract and getattr(contract, "peak_daily_volume_gb", 0.0) > 0:
            available.append("peak_volume_specification")
        elif contract and getattr(contract, "scalability", None) and getattr(contract.scalability, "peak_forecast_gb_per_day", 0.0) > 0:
            available.append("peak_volume_specification")
        else:
            unavailable.append("peak_volume_specification")

        if "baseline_volume" in available:
            available.append("growth_projections")
        else:
            unavailable.append("growth_projections")

        if runtime_data or (historical_runs and len(historical_runs) >= 2):
            available.append("cluster_headroom")
            if runtime_data:
                provenance.append(EvidenceProvenanceKind.RUNTIME)
            if historical_runs:
                provenance.append(EvidenceProvenanceKind.HISTORICAL_RUN)
        else:
            unavailable.append("cluster_headroom")

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "baseline_volume" in available and "growth_projections" in available
        quality = EvidenceQuality.STRONG if comp >= 0.75 else (
            EvidenceQuality.MODERATE if comp >= 0.5 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if "cluster_headroom" in available else (
            ConfidenceLevel.MEDIUM if sufficient else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Baseline and contractual peak workload volume specifications")

        return DomainEvidenceCoverage(
            domain="scalability",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.CURRENT if runtime_data else EvidenceFreshness.STATIC,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Scalability modeling evidence present ({len(available)}/{len(expected)} inputs)"
                if sufficient
                else "Insufficient workload volume bounds for scalability analysis"
            ),
        )

    def _evaluate_security_domain(
        self, analysis: Any | None, contract: PipelineContract | None
    ) -> DomainEvidenceCoverage:
        expected = ["code_secret_scan", "contract_secret_scan", "credential_redaction"]
        available = ["code_secret_scan", "contract_secret_scan", "credential_redaction"]
        comp = 1.0
        sufficient = True
        quality = EvidenceQuality.STRONG
        conf = ConfidenceLevel.HIGH

        return DomainEvidenceCoverage(
            domain="security",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=[],
            provenance=[EvidenceProvenanceKind.STATIC_CODE, EvidenceProvenanceKind.CONTRACT],
            freshness=EvidenceFreshness.STATIC,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=[],
            summary="Complete static security & credential redaction audit completed",
        )

    def _evaluate_governance_domain(self, contract: PipelineContract | None) -> DomainEvidenceCoverage:
        expected = ["target_catalog_declaration", "pipeline_owner", "data_lineage_declaration"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        if contract:
            provenance.append(EvidenceProvenanceKind.CONTRACT)
            if contract.target and contract.target.path:
                available.append("target_catalog_declaration")
            else:
                unavailable.append("target_catalog_declaration")
            if getattr(contract, "owner", None) or getattr(contract, "developer", None):
                available.append("pipeline_owner")
            else:
                unavailable.append("pipeline_owner")
            if getattr(contract, "lineage", None) or (contract.source and contract.target):
                available.append("data_lineage_declaration")
            else:
                unavailable.append("data_lineage_declaration")
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "target_catalog_declaration" in available
        quality = EvidenceQuality.STRONG if comp >= 0.66 else (
            EvidenceQuality.MODERATE if comp > 0 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if comp >= 0.66 else (
            ConfidenceLevel.MEDIUM if comp > 0 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Target catalog declaration and ownership metadata")

        return DomainEvidenceCoverage(
            domain="governance",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.STATIC if available else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Governance catalog evidence present ({len(available)}/{len(expected)} items)"
                if sufficient
                else "Insufficient governance metadata"
            ),
        )

    def _evaluate_cost_domain(
        self, runtime_data: Any | None, cluster_config: dict[str, Any] | None
    ) -> DomainEvidenceCoverage:
        expected = ["dbu_pricing_rate", "cluster_operational_hours", "pricing_tier_metadata"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        has_dbu = False
        if runtime_data and getattr(runtime_data, "dbu_consumed", None) is not None:
            available.append("dbu_pricing_rate")
            has_dbu = True
            provenance.append(EvidenceProvenanceKind.RUNTIME)
        else:
            unavailable.append("dbu_pricing_rate")

        if runtime_data and getattr(runtime_data, "duration_seconds", None) is not None:
            available.append("cluster_operational_hours")
            if EvidenceProvenanceKind.RUNTIME not in provenance:
                provenance.append(EvidenceProvenanceKind.RUNTIME)
        else:
            unavailable.append("cluster_operational_hours")

        if cluster_config and "pricing_tier" in cluster_config:
            available.append("pricing_tier_metadata")
            provenance.append(EvidenceProvenanceKind.JOB_CONFIG)
        else:
            unavailable.append("pricing_tier_metadata")

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = has_dbu and "cluster_operational_hours" in available
        quality = EvidenceQuality.STRONG if comp >= 0.75 else (
            EvidenceQuality.MODERATE if comp > 0 else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if sufficient else (
            ConfidenceLevel.LOW if comp > 0 else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Databricks Unit (DBU) consumption rates and pricing tier metadata")

        return DomainEvidenceCoverage(
            domain="cost",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.CURRENT if has_dbu else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                "Cost telemetry established"
                if sufficient
                else "DBU and cost telemetry unavailable (Cost analysis UNKNOWN)"
            ),
        )

    def _evaluate_implementation_domain(
        self, impl_assessment: ImplementationForensicsResult | None
    ) -> DomainEvidenceCoverage:
        expected = ["ast_transformations_inspected", "anti_pattern_findings", "write_mode_inspected"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        if impl_assessment:
            provenance.append(EvidenceProvenanceKind.STATIC_CODE)
            available.extend(expected)
            comp = 1.0
            sufficient = True
            quality = EvidenceQuality.STRONG
            conf = ConfidenceLevel.HIGH
            summary = f"Implementation forensics evaluated ({len(impl_assessment.all_findings)} anti-patterns detected)"
        else:
            unavailable.extend(expected)
            comp = 0.0
            sufficient = False
            quality = EvidenceQuality.INSUFFICIENT
            conf = ConfidenceLevel.INSUFFICIENT
            summary = "Implementation forensics not evaluated"

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Developer implementation forensics AST evaluation")

        return DomainEvidenceCoverage(
            domain="implementation_forensics",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.STATIC if available else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=summary,
        )

    def _evaluate_rerun_domain(
        self, rerun_assessment: RerunAnalysisResult | None
    ) -> DomainEvidenceCoverage:
        expected = ["idempotency_proof", "merge_key_safety_evidence", "duplicate_risk_analysis"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        sufficient = False
        quality = EvidenceQuality.INSUFFICIENT
        conf = ConfidenceLevel.INSUFFICIENT

        if rerun_assessment:
            provenance.append(EvidenceProvenanceKind.STATIC_CODE)
            available.append("duplicate_risk_analysis")

            # Check if idempotency is conclusive or UNKNOWN due to missing key evidence
            idm_status = getattr(rerun_assessment.idempotency, "overall_status", None) or getattr(rerun_assessment.idempotency, "status", None)
            if idm_status in (CheckpointStatus.PASS, CheckpointStatus.FAIL):
                available.append("idempotency_proof")
                available.append("merge_key_safety_evidence")
                sufficient = True
                quality = EvidenceQuality.STRONG
                conf = ConfidenceLevel.HIGH
            elif idm_status == CheckpointStatus.WARN:
                available.append("idempotency_proof")
                sufficient = True
                quality = EvidenceQuality.MODERATE
                conf = ConfidenceLevel.MEDIUM
            else:
                # UNKNOWN
                unavailable.append("idempotency_proof")
                unavailable.append("merge_key_safety_evidence")
                sufficient = False
                quality = EvidenceQuality.WEAK
                conf = ConfidenceLevel.LOW
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0

        req_evidence: list[str] = []
        if not sufficient:
            req_evidence.append("Explicit contract uniqueness declaration or source deduplication key evidence")

        return DomainEvidenceCoverage(
            domain="rerun_idempotency",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.STATIC if available else EvidenceFreshness.UNKNOWN,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Rerun idempotency forensics complete (Status: {idm_status.value if idm_status else 'UNKNOWN'})"
                if rerun_assessment and sufficient
                else "Rerun idempotency unproven (MERGE key ambiguity or missing uniqueness evidence)"
            ),
        )

    def _evaluate_alignment_domain(
        self, alignment_assessment: ThreeLayerAlignmentAssessment | None
    ) -> DomainEvidenceCoverage:
        expected = ["nine_dimensions_evaluated", "layer_divergences_identified", "actual_telemetry_reconciled"]
        available: list[str] = []
        unavailable: list[str] = []
        provenance: list[EvidenceProvenanceKind] = []

        if alignment_assessment:
            provenance.append(EvidenceProvenanceKind.CONTRACT)
            provenance.append(EvidenceProvenanceKind.STATIC_CODE)
            available.append("nine_dimensions_evaluated")
            available.append("layer_divergences_identified")

            # Check if any actual runtime dimension was evaluated
            has_actual = any(
                "UNKNOWN" not in f.actual for f in alignment_assessment.findings
            )
            if has_actual:
                available.append("actual_telemetry_reconciled")
                provenance.append(EvidenceProvenanceKind.RUNTIME)
            else:
                unavailable.append("actual_telemetry_reconciled")
        else:
            unavailable.extend(expected)

        comp = len(available) / len(expected) if expected else 0.0
        sufficient = "nine_dimensions_evaluated" in available
        quality = EvidenceQuality.STRONG if "actual_telemetry_reconciled" in available else (
            EvidenceQuality.MODERATE if sufficient else EvidenceQuality.INSUFFICIENT
        )
        conf = ConfidenceLevel.HIGH if "actual_telemetry_reconciled" in available else (
            ConfidenceLevel.MEDIUM if sufficient else ConfidenceLevel.INSUFFICIENT
        )

        req_evidence: list[str] = []
        if "actual_telemetry_reconciled" not in available:
            req_evidence.append("Live or historical runtime execution metrics for Layer 3 (ACTUAL) reconciliation")

        return DomainEvidenceCoverage(
            domain="three_layer_alignment",
            evidence_expected=expected,
            evidence_available=available,
            evidence_unavailable=unavailable,
            provenance=provenance,
            freshness=EvidenceFreshness.CURRENT if "actual_telemetry_reconciled" in available else EvidenceFreshness.STATIC,
            quality=quality,
            completeness_score=comp,
            confidence=conf,
            decision_sufficient=sufficient,
            required_evidence_for_sufficiency=req_evidence,
            summary=(
                f"Three-layer alignment complete ({alignment_assessment.total_drifts} drifts identified)"
                if alignment_assessment
                else "Three-layer alignment not evaluated"
            ),
        )

    # -------------------------------------------------------------------------
    # Key Pipeline Decisions Evaluator
    # -------------------------------------------------------------------------

    def _evaluate_key_decisions(
        self,
        domain_coverages: dict[str, DomainEvidenceCoverage],
        contract: PipelineContract | None,
        runtime_data: Any | None,
        rerun_assessment: RerunAnalysisResult | None,
        alignment_assessment: ThreeLayerAlignmentAssessment | None,
        checkpoints: dict[str, Checkpoint],
    ) -> list[DecisionSufficiencyRecord]:
        decisions: list[DecisionSufficiencyRecord] = []

        # 1. SLA Compliance Decision
        sla_cp = checkpoints.get("CP-023") or checkpoints.get("sla")
        has_runtime_dur = domain_coverages["runtime"].decision_sufficient
        if has_runtime_dur:
            status = "PASS" if (sla_cp and sla_cp.status == CheckpointStatus.PASS) else "FAIL"
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="SLA_COMPLIANCE",
                    domain="sla",
                    decision_status=status,
                    is_sufficient=True,
                    confidence=ConfidenceLevel.HIGH,
                    supporting_evidence=["Runtime execution duration telemetry"],
                    missing_evidence=[],
                    required_evidence=[],
                    rationale="Observed runtime duration directly evaluated against contractual SLA boundary.",
                )
            )
        else:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="SLA_COMPLIANCE",
                    domain="sla",
                    decision_status="UNKNOWN",
                    is_sufficient=False,
                    confidence=ConfidenceLevel.INSUFFICIENT,
                    supporting_evidence=[],
                    missing_evidence=["Runtime execution duration telemetry"],
                    required_evidence=["Observed runtime duration telemetry"],
                    rationale="SLA compliance cannot be determined without runtime duration telemetry.",
                )
            )

        # 2. Idempotency Safety Decision
        if rerun_assessment:
            idm = rerun_assessment.idempotency
            idm_status = getattr(idm, "overall_status", None) or getattr(idm, "status", None)
            if idm_status in (CheckpointStatus.PASS, CheckpointStatus.FAIL):
                decisions.append(
                    DecisionSufficiencyRecord(
                        decision_name="IDEMPOTENCY_SAFETY",
                        domain="rerun_idempotency",
                        decision_status=idm_status.value,
                        is_sufficient=True,
                        confidence=ConfidenceLevel.HIGH,
                        supporting_evidence=[f"Definitive idempotency evaluation: {idm_status.value}"],
                        missing_evidence=[],
                        required_evidence=[],
                        rationale="Target write mode and key uniqueness conclusively proven.",
                    )
                )
            elif idm_status == CheckpointStatus.WARN:
                decisions.append(
                    DecisionSufficiencyRecord(
                        decision_name="IDEMPOTENCY_SAFETY",
                        domain="rerun_idempotency",
                        decision_status="WARN",
                        is_sufficient=True,
                        confidence=ConfidenceLevel.MEDIUM,
                        supporting_evidence=["Static write mode analysis"],
                        missing_evidence=["Explicit target constraint validation"],
                        required_evidence=[],
                        rationale="Idempotency qualified warning: partial duplicate protection detected.",
                    )
                )
            else:
                # UNKNOWN
                decisions.append(
                    DecisionSufficiencyRecord(
                        decision_name="IDEMPOTENCY_SAFETY",
                        domain="rerun_idempotency",
                        decision_status="UNKNOWN",
                        is_sufficient=False,
                        confidence=ConfidenceLevel.LOW,
                        supporting_evidence=["Static AST code inspection"],
                        missing_evidence=["Key uniqueness proof", "Source deduplication evidence"],
                        required_evidence=["Explicit contract key uniqueness declaration or source deduplication key"],
                        rationale="MERGE key stability/uniqueness unproven; idempotency cannot be certified safe.",
                    )
                )
        else:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="IDEMPOTENCY_SAFETY",
                    domain="rerun_idempotency",
                    decision_status="UNKNOWN",
                    is_sufficient=False,
                    confidence=ConfidenceLevel.INSUFFICIENT,
                    supporting_evidence=[],
                    missing_evidence=["Rerun idempotency forensics"],
                    required_evidence=["Rerun forensics AST and contract key analysis"],
                    rationale="Rerun analysis was not performed.",
                )
            )

        # 3. Cost Viability Decision
        cost_cov = domain_coverages["cost"]
        if cost_cov.decision_sufficient:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="COST_VIABILITY",
                    domain="cost",
                    decision_status="PASS",
                    is_sufficient=True,
                    confidence=ConfidenceLevel.HIGH,
                    supporting_evidence=["DBU consumption telemetry", "Cluster duration hours"],
                    missing_evidence=[],
                    required_evidence=[],
                    rationale="Complete DBU spend and compute duration telemetry available.",
                )
            )
        else:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="COST_VIABILITY",
                    domain="cost",
                    decision_status="UNKNOWN",
                    is_sufficient=False,
                    confidence=ConfidenceLevel.INSUFFICIENT,
                    supporting_evidence=[],
                    missing_evidence=["DBU consumption rate", "Pricing tier metadata"],
                    required_evidence=["Databricks Unit (DBU) consumption rates and pricing tier metadata"],
                    rationale="Cost projection unavailable in offline static analysis without DBU consumption telemetry.",
                )
            )

        # 4. Scalability at Peak Decision
        scal_cov = domain_coverages["scalability"]
        if scal_cov.decision_sufficient and domain_coverages["historical_runs"].decision_sufficient:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="SCALABILITY_AT_PEAK",
                    domain="scalability",
                    decision_status="PASS",
                    is_sufficient=True,
                    confidence=ConfidenceLevel.HIGH,
                    supporting_evidence=["Historical multi-run scaling series", "Contract peak workload bounds"],
                    missing_evidence=[],
                    required_evidence=[],
                    rationale="Empirical multi-run scaling trend confirms cluster capacity headroom.",
                )
            )
        elif scal_cov.decision_sufficient:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="SCALABILITY_AT_PEAK",
                    domain="scalability",
                    decision_status="WARN",
                    is_sufficient=True,
                    confidence=ConfidenceLevel.MEDIUM,
                    supporting_evidence=["Static profile and contract volume bounds"],
                    missing_evidence=["Empirical multi-run historical telemetry"],
                    required_evidence=["At least 2 historical runs under scale to confirm headroom without spill"],
                    rationale="Static volume bounds modeled, but empirical scale behavior unverified.",
                )
            )
        else:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="SCALABILITY_AT_PEAK",
                    domain="scalability",
                    decision_status="UNKNOWN",
                    is_sufficient=False,
                    confidence=ConfidenceLevel.INSUFFICIENT,
                    supporting_evidence=[],
                    missing_evidence=["Workload volume bounds", "Cluster scaling limits"],
                    required_evidence=["Baseline and peak volume specifications in contract or profile"],
                    rationale="Scalability cannot be evaluated without workload volume bounds.",
                )
            )

        # 5. Configuration Alignment Decision
        if alignment_assessment:
            blocking_drifts = sum(1 for f in alignment_assessment.findings if f.blocking)
            has_actual = domain_coverages["three_layer_alignment"].quality == EvidenceQuality.STRONG
            status = "FAIL" if (
                blocking_drifts > 0
                or alignment_assessment.overall_status == CheckpointStatus.FAIL
                or alignment_assessment.drift_severity == DriftSeverity.BLOCKING
            ) else (
                "WARN" if alignment_assessment.total_drifts > 0 else "PASS"
            )
            conf = ConfidenceLevel.HIGH if has_actual else ConfidenceLevel.MEDIUM
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="CONFIGURATION_ALIGNMENT",
                    domain="three_layer_alignment",
                    decision_status=status,
                    is_sufficient=True,
                    confidence=conf,
                    supporting_evidence=[
                        f"Evaluated 9 dimensions: {alignment_assessment.total_drifts} drift(s) found"
                    ],
                    missing_evidence=[] if has_actual else ["Live workspace environment configuration"],
                    required_evidence=[] if has_actual else ["Live Databricks workspace inspection for Layer 3 (ACTUAL)"],
                    rationale=(
                        f"Reconciliation completed across all 9 dimensions ({blocking_drifts} blocking drifts)."
                    ),
                )
            )
        else:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="CONFIGURATION_ALIGNMENT",
                    domain="three_layer_alignment",
                    decision_status="UNKNOWN",
                    is_sufficient=False,
                    confidence=ConfidenceLevel.INSUFFICIENT,
                    supporting_evidence=[],
                    missing_evidence=["Three-layer alignment forensics"],
                    required_evidence=["M5G ThreeLayerAlignment assessment"],
                    rationale="Three-layer alignment was not evaluated.",
                )
            )

        # 6. Production Release Decision
        blocking_cps = [
            cp for cp in checkpoints.values() if cp.status == CheckpointStatus.FAIL and cp.severity.value in ("CRITICAL", "HIGH")
        ]
        has_blocking = len(blocking_cps) > 0
        missing_critical_domains = [
            d.domain for d in (domain_coverages["source"], domain_coverages["code"], domain_coverages["pipeline"])
            if not d.decision_sufficient
        ]

        if has_blocking or missing_critical_domains:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="PRODUCTION_RELEASE",
                    domain="readiness",
                    decision_status="FAIL",
                    is_sufficient=True,
                    confidence=ConfidenceLevel.HIGH,
                    supporting_evidence=[f"{len(blocking_cps)} blocking checkpoint failure(s) detected"],
                    missing_evidence=[],
                    required_evidence=[],
                    rationale="Definitive release rejection: blocking failures or missing core contract/code.",
                )
            )
        elif not has_runtime_dur:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="PRODUCTION_RELEASE",
                    domain="readiness",
                    decision_status="UNKNOWN",
                    is_sufficient=False,
                    confidence=ConfidenceLevel.LOW,
                    supporting_evidence=["Static contract and code validation completed cleanly"],
                    missing_evidence=["Runtime execution telemetry", "Cost & SLA compliance proof"],
                    required_evidence=["Runtime execution telemetry to certify SLA and cost compliance"],
                    rationale="Pipeline passed static checks, but lacks runtime telemetry to certify production release.",
                )
            )
        else:
            decisions.append(
                DecisionSufficiencyRecord(
                    decision_name="PRODUCTION_RELEASE",
                    domain="readiness",
                    decision_status="PASS",
                    is_sufficient=True,
                    confidence=ConfidenceLevel.HIGH,
                    supporting_evidence=["Complete static and runtime evidence evaluated cleanly"],
                    missing_evidence=[],
                    required_evidence=[],
                    rationale="Full evidence sufficiency established across static and runtime domains.",
                )
            )

        # 7. Task Coverage Decision (Phase 8): every enumerated job task must
        # be accounted for. Uncovered tasks (UNSUPPORTED/UNRETRIEVABLE) are
        # incomplete evidence (UNKNOWN, never FAIL); fully covered topologies
        # add a passing decision with no missing evidence.
        topology = (self.context or {}).get("task_topology") or []
        if isinstance(topology, list) and topology:
            uncovered_names: list[str] = []
            for task in topology:
                if (
                    isinstance(task, dict)
                    and task.get("coverage_state") != "ANALYZED"
                ):
                    uncovered_names.append(
                        f"{task.get('task_key', 'task')} "
                        f"[{task.get('coverage_state', 'UNKNOWN')}]"
                    )
            if uncovered_names:
                decisions.append(
                    DecisionSufficiencyRecord(
                        decision_name="TASK_COVERAGE",
                        domain="job",
                        decision_status="UNKNOWN",
                        is_sufficient=False,
                        confidence=ConfidenceLevel.LOW,
                        supporting_evidence=[],
                        missing_evidence=[
                            f"Task validation coverage for: {', '.join(uncovered_names)}"
                        ],
                        required_evidence=[
                            "Code retrieval or explicit unsupported classification "
                            f"for uncovered job tasks: {', '.join(uncovered_names)}"
                        ],
                        rationale=(
                            "One or more job tasks were not analyzed; the job "
                            "cannot be certified as fully validated."
                        ),
                    )
                )
            else:
                decisions.append(
                    DecisionSufficiencyRecord(
                        decision_name="TASK_COVERAGE",
                        domain="job",
                        decision_status="PASS",
                        is_sufficient=True,
                        confidence=ConfidenceLevel.HIGH,
                        supporting_evidence=[
                            "All enumerated job tasks analyzed: "
                            + ", ".join(
                                str(t.get("task_key", "task"))
                                for t in topology
                                if isinstance(t, dict)
                            )
                        ],
                        missing_evidence=[],
                        required_evidence=[],
                        rationale="Every discovered job task has analyzed code evidence.",
                    )
                )

        return decisions
