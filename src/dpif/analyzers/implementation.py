"""Analyzers for Developer Implementation Forensics (M5E).

Correlates Python/PySpark/SQL code analysis, flow analysis, and available
runtime evidence into implementation forensic findings across 9 dimensions:
1. Transformation Quality & Flow
2. Pre-Shuffle Optimization
3. Partitioning & Parallelism Strategy
4. Join Analysis & Strategy
5. Cache & Persist Lifecycle
6. Checkpoint Lifecycle
7. Resource & Process Lifecycle
8. Exception Safety & Swallowed Exceptions
9. Implementation Completeness & Process Finalization
"""

from __future__ import annotations

import ast
from typing import Any

from dpif.code import flow as flow_mod
from dpif.code.models import CodeAnalysis, Operation, OperationType
from dpif.models import CheckpointStatus, Severity
from dpif.models.implementation import (
    DimensionAssessment,
    EvidenceProvenanceKind,
    ForensicFinding,
    ImplementationDimension,
    ImplementationForensicsResult,
)

SHUFFLE_TRIGGER_OPS = frozenset(
    {
        OperationType.JOIN,
        OperationType.GROUP_BY,
        OperationType.AGGREGATE,
        OperationType.DISTINCT,
        OperationType.DROP_DUPLICATES,
        OperationType.ORDER_BY,
        OperationType.REPARTITION,
        OperationType.WINDOW,
        OperationType.UNION,
    }
)


def _get_volume(context: dict[str, Any]) -> float | None:
    for key in ("data_size_gb", "table_size_gb", "expected_volume_gb", "peak_volume_gb"):
        val = context.get(key)
        if isinstance(val, (int, float)):
            return float(val)
    return None


def _get_provenance(context: dict[str, Any]) -> EvidenceProvenanceKind:
    ev_src = str(context.get("evidence_source") or "").lower()
    coll_method = str(context.get("collection_method") or "").lower()
    if "runtime" in ev_src or "runtime" in coll_method:
        return EvidenceProvenanceKind.RUNTIME
    if "historical" in ev_src or "historical" in coll_method:
        return EvidenceProvenanceKind.HISTORICAL_RUN
    if "fixture" in ev_src or "fixture" in coll_method:
        return EvidenceProvenanceKind.FIXTURE
    if "contract" in ev_src:
        return EvidenceProvenanceKind.CONTRACT
    if "metadata" in ev_src:
        return EvidenceProvenanceKind.METADATA
    return EvidenceProvenanceKind.STATIC_CODE


class DeveloperImplementationAnalyzer:
    """Forensic analyzer assessing developer implementation decisions (M5E)."""

    def __init__(self, code_analysis: CodeAnalysis, context: dict[str, Any] | None = None) -> None:
        self.code = code_analysis
        self.context = context or {}
        self.provenance = _get_provenance(self.context)
        self.volume_gb = _get_volume(self.context)
        self.raw_source = getattr(code_analysis, "_raw_source", "") or ""

    def analyze(self) -> ImplementationForensicsResult:
        pipeline_name = str(self.context.get("pipeline_name", "unknown_pipeline"))
        findings: list[ForensicFinding] = []

        findings.extend(self._analyze_transformations())
        findings.extend(self._analyze_shuffle_optimization())
        findings.extend(self._analyze_partitioning())
        findings.extend(self._analyze_joins())
        findings.extend(self._analyze_cache_lifecycle())
        findings.extend(self._analyze_checkpoint_lifecycle())
        findings.extend(self._analyze_resource_lifecycle())
        findings.extend(self._analyze_exception_safety())
        findings.extend(self._analyze_completeness())

        # Group findings into 9 dimensions
        dim_assessments: dict[str, DimensionAssessment] = {}

        for dim in ImplementationDimension:
            dim_findings = [f for f in findings if f.dimension == dim]

            # Compute dimension status
            if not dim_findings:
                status = CheckpointStatus.PASS
                max_sev = Severity.INFO
                summary = "No issues detected."
            else:
                has_fail = any(f.status == CheckpointStatus.FAIL for f in dim_findings)
                has_warn = any(f.status == CheckpointStatus.WARN for f in dim_findings)
                has_unknown = any(f.status == CheckpointStatus.UNKNOWN for f in dim_findings)

                if has_fail:
                    status = CheckpointStatus.FAIL
                elif has_warn:
                    status = CheckpointStatus.WARN
                elif has_unknown:
                    status = CheckpointStatus.UNKNOWN
                else:
                    status = CheckpointStatus.PASS

                severities = [f.severity for f in dim_findings]
                if Severity.CRITICAL in severities:
                    max_sev = Severity.CRITICAL
                elif Severity.HIGH in severities:
                    max_sev = Severity.HIGH
                elif Severity.MEDIUM in severities:
                    max_sev = Severity.MEDIUM
                elif Severity.LOW in severities:
                    max_sev = Severity.LOW
                else:
                    max_sev = Severity.INFO

                summary = f"Evaluated {len(dim_findings)} forensic finding(s)."

            dim_assessments[dim.value] = DimensionAssessment(
                dimension=dim,
                status=status,
                findings_count=len(dim_findings),
                max_severity=max_sev,
                summary=summary,
                findings=dim_findings,
            )

        # Overall status
        all_statuses = [da.status for da in dim_assessments.values()]
        if CheckpointStatus.FAIL in all_statuses:
            overall = CheckpointStatus.FAIL
        elif CheckpointStatus.WARN in all_statuses:
            overall = CheckpointStatus.WARN
        elif CheckpointStatus.UNKNOWN in all_statuses:
            overall = CheckpointStatus.UNKNOWN
        else:
            overall = CheckpointStatus.PASS

        return ImplementationForensicsResult(
            pipeline_name=pipeline_name,
            overall_status=overall,
            dimensions=dim_assessments,
            all_findings=findings,
        )

    # -------------------------------------------------------------------------
    # 1. Transformation Quality & Flow
    # -------------------------------------------------------------------------
    def _analyze_transformations(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        ops = self.code.operations

        # Check UDF usage
        udfs = [o for o in ops if o.operation_type in (OperationType.UDF, OperationType.PANDAS_UDF)]
        for udf in udfs:
            is_pandas = udf.operation_type == OperationType.PANDAS_UDF
            title = "Pandas UDF Implemented" if is_pandas else "Python UDF Implemented"
            rule_id = "IMP-TRANS-001" if not is_pandas else "IMP-TRANS-002"
            loc = udf.location(self.code.source_file)

            if not is_pandas and self.volume_gb is not None and self.volume_gb >= 100.0:
                status = CheckpointStatus.WARN
                sev = Severity.HIGH
                desc = (
                    f"Python UDF implementation on large input (~{self.volume_gb:.0f} GB) "
                    "causes PySpark row-wise serialization overhead."
                )
                rec = "Consider refactoring to native PySpark SQL functions or Vectorized Pandas UDF."
            elif is_pandas:
                status = CheckpointStatus.PASS
                sev = Severity.INFO
                desc = "Vectorized Pandas UDF implemented, providing efficient Arrow serialization."
                rec = "Ensure batch sizes are tuned if running near memory limits."
            else:
                status = CheckpointStatus.WARN
                sev = Severity.MEDIUM
                desc = "Python UDF implementation detected."
                rec = "Prefer native PySpark DataFrame expressions where possible."

            findings.append(
                ForensicFinding(
                    finding_id=f"{rule_id}:{udf.line}",
                    rule_id=rule_id,
                    dimension=ImplementationDimension.TRANSFORMATION_QUALITY,
                    title=title,
                    description=desc,
                    status=status,
                    severity=sev,
                    observed={"line": udf.line, "code": udf.code, "input_gb": self.volume_gb},
                    expected={"native_or_vectorized_functions": True},
                    evidence=[f"UDF operation '{udf.code}' at line {udf.line}"],
                    recommendation=rec,
                    confidence=0.85 if is_pandas else 0.75,
                    provenance=self.provenance,
                    location=loc,
                )
            )

        # Check for count() for existence
        count_ops = [o for o in ops if o.operation_type == OperationType.COUNT]
        for c_op in count_ops:
            is_existence = bool(
                c_op.arguments.get("existence_check") or c_op.context.get("existence_check")
            )
            if is_existence:
                loc = c_op.location(self.code.source_file)
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-TRANS-003:{c_op.line}",
                        rule_id="IMP-TRANS-003",
                        dimension=ImplementationDimension.TRANSFORMATION_QUALITY,
                        title="df.count() Used for Existence Check",
                        description=(
                            "Developer implemented df.count() in a comparison/boolean condition "
                            "to check for dataset existence/emptiness."
                        ),
                        status=CheckpointStatus.WARN,
                        severity=Severity.HIGH if (self.volume_gb and self.volume_gb >= 100.0) else Severity.MEDIUM,
                        observed={
                            "line": c_op.line,
                            "dataframe": c_op.dataframe,
                            "code": c_op.code,
                        },
                        expected={"existence_check_method": "df.isEmpty() or df.limit(1).count() > 0"},
                        evidence=[f"count() call evaluated as boolean condition at line {c_op.line}"],
                        recommendation="Use df.isEmpty() or df.limit(1).count() > 0 to avoid a full dataset scan.",
                        confidence=0.9,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

        return findings

    # -------------------------------------------------------------------------
    # 2. Pre-Shuffle Optimization
    # -------------------------------------------------------------------------
    def _analyze_shuffle_optimization(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        ops = self.code.operations

        # Find joins and filters
        joins = [o for o in ops if o.operation_type == OperationType.JOIN]
        filters = [o for o in ops if o.operation_type == OperationType.FILTER]

        for j_op in joins:
            loc = j_op.location(self.code.source_file)
            j_df = j_op.dataframe

            # Check if filter occurs before join vs after join on the same df flow
            filters_before = [
                f for f in filters if f.dataframe == j_df and f.line < j_op.line
            ]
            filters_after = [
                f for f in filters if f.dataframe == j_df and f.line > j_op.line
            ]

            if filters_before:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-SHUFFLE-001:{j_op.line}",
                        rule_id="IMP-SHUFFLE-001",
                        dimension=ImplementationDimension.SHUFFLE_OPTIMIZATION,
                        title="Pre-Join Filter Optimization Implemented",
                        description="Filter applied prior to join operation, potentially reducing join shuffle input.",
                        status=CheckpointStatus.PASS,
                        severity=Severity.INFO,
                        observed={
                            "join_line": j_op.line,
                            "filter_lines": [f.line for f in filters_before],
                            "dataframe": j_df,
                        },
                        expected={"pre_shuffle_filtering": True},
                        evidence=[
                            f"Filter at line {filters_before[0].line} precedes join at line {j_op.line} on '{j_df}'"
                        ],
                        recommendation="Maintain pre-join filtering pattern.",
                        confidence=0.85,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            elif filters_after:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-SHUFFLE-002:{j_op.line}",
                        rule_id="IMP-SHUFFLE-002",
                        dimension=ImplementationDimension.SHUFFLE_OPTIMIZATION,
                        title="Post-Join Filtering (Potential Pre-Shuffle Optimization Missing)",
                        description=(
                            "Filter applied after join operation. If the filter condition relies "
                            "only on input columns, moving it before the join could reduce join input."
                        ),
                        status=CheckpointStatus.WARN,
                        severity=Severity.MEDIUM,
                        observed={
                            "join_line": j_op.line,
                            "filter_lines": [f.line for f in filters_after],
                            "dataframe": j_df,
                        },
                        expected={"pre_shuffle_filtering": "preferred when semantically safe"},
                        evidence=[
                            f"Join at line {j_op.line} precedes filter at line {filters_after[0].line} on '{j_df}'"
                        ],
                        recommendation="Evaluate whether filter condition can be pushed down before the join.",
                        confidence=0.7,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

            # Check for repartition directly before join
            reparts = [
                o for o in ops if o.operation_type == OperationType.REPARTITION and o.line < j_op.line
            ]
            if reparts and (j_op.line - reparts[-1].line <= 3):
                r_op = reparts[-1]
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-SHUFFLE-003:{r_op.line}",
                        rule_id="IMP-SHUFFLE-003",
                        dimension=ImplementationDimension.SHUFFLE_OPTIMIZATION,
                        title="Repartition Immediately Preceding Join",
                        description=(
                            "Explicit repartition() immediately precedes a join operation. "
                            "This may cause a double shuffle if the repartition keys do not match the join keys."
                        ),
                        status=CheckpointStatus.WARN,
                        severity=Severity.MEDIUM,
                        observed={"repartition_line": r_op.line, "join_line": j_op.line},
                        expected={"single_shuffle_or_matching_partitioning": True},
                        evidence=[
                            f"repartition() at line {r_op.line} immediately before join at line {j_op.line}"
                        ],
                        recommendation="Verify if explicit repartition before join is necessary or if Spark's join shuffle suffices.",
                        confidence=0.75,
                        provenance=self.provenance,
                        location=r_op.location(self.code.source_file),
                    )
                )

        return findings

    # -------------------------------------------------------------------------
    # 3. Partitioning & Parallelism
    # -------------------------------------------------------------------------
    def _analyze_partitioning(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        reparts = self.code.of_type(OperationType.REPARTITION)
        coalesces = self.code.of_type(OperationType.COALESCE)

        # Repeated repartitioning on the same DataFrame flow
        by_flow: dict[str, list[Operation]] = {}
        for r in reparts:
            root = flow_mod.flow_root(self.code, r.dataframe or "<unknown>", r.line)
            by_flow.setdefault(root, []).append(r)

        for root, r_ops in by_flow.items():
            if len(r_ops) >= 2:
                last_op = r_ops[-1]
                loc = last_op.location(self.code.source_file)
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-PART-001:{last_op.line}",
                        rule_id="IMP-PART-001",
                        dimension=ImplementationDimension.PARTITIONING_QUALITY,
                        title="Repeated Repartition Operations",
                        description=f"Multiple repartition() calls ({len(r_ops)}) detected on dataset flow '{root}'.",
                        status=CheckpointStatus.WARN,
                        severity=Severity.MEDIUM,
                        observed={
                            "flow_root": root,
                            "repartition_count": len(r_ops),
                            "lines": [o.line for o in r_ops],
                        },
                        expected={"repartition_count": "<= 1 per flow"},
                        evidence=[f"{len(r_ops)} repartition calls at lines {[o.line for o in r_ops]}"],
                        recommendation="Consolidate repartitioning to avoid unnecessary intermediate full shuffles.",
                        confidence=0.8,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

        # Coalesce analysis (coalesce(1) before aggregation vs after aggregation before write)
        aggs = set(self.code.of_type(OperationType.GROUP_BY, OperationType.AGGREGATE))
        writes = set(self.code.of_type(OperationType.WRITE))

        for c_op in coalesces:
            loc = c_op.location(self.code.source_file)
            lits = c_op.arguments.get("literals", [])
            is_single = isinstance(lits, list) and 1 in lits

            if is_single:
                has_agg_after = any(a.line > c_op.line for a in aggs if a.dataframe == c_op.dataframe)
                has_write_after = any(w.line > c_op.line for w in writes)

                if has_agg_after:
                    findings.append(
                        ForensicFinding(
                            finding_id=f"IMP-PART-002:{c_op.line}",
                            rule_id="IMP-PART-002",
                            dimension=ImplementationDimension.PARTITIONING_QUALITY,
                            title="coalesce(1) Preceding Aggregation",
                            description="coalesce(1) reduces dataset to a single partition prior to an aggregation, crippling parallelism.",
                            status=CheckpointStatus.FAIL,
                            severity=Severity.HIGH,
                            observed={"line": c_op.line, "code": c_op.code},
                            expected={"parallel_processing_before_agg": True},
                            evidence=[f"coalesce(1) at line {c_op.line} followed by downstream aggregation"],
                            recommendation="Remove coalesce(1) before aggregation or move coalesce to final output stage.",
                            confidence=0.85,
                            provenance=self.provenance,
                            location=loc,
                        )
                    )
                elif has_write_after:
                    findings.append(
                        ForensicFinding(
                            finding_id=f"IMP-PART-003:{c_op.line}",
                            rule_id="IMP-PART-003",
                            dimension=ImplementationDimension.PARTITIONING_QUALITY,
                            title="coalesce(1) Used Prior to Output Write",
                            description="coalesce(1) used to produce a single output file.",
                            status=CheckpointStatus.PASS if (self.volume_gb and self.volume_gb < 1.0) else CheckpointStatus.WARN,
                            severity=Severity.INFO if (self.volume_gb and self.volume_gb < 1.0) else Severity.MEDIUM,
                            observed={"line": c_op.line, "code": c_op.code, "volume_gb": self.volume_gb},
                            expected={"single_partition_output": "reasonable for small result sets"},
                            evidence=[f"coalesce(1) at line {c_op.line} before write"],
                            recommendation="Verify output volume is small (<1 GB) so single-thread writer does not bottleneck.",
                            confidence=0.8,
                            provenance=self.provenance,
                            location=loc,
                        )
                    )

        return findings

    # -------------------------------------------------------------------------
    # 4. Join Analysis & Strategy
    # -------------------------------------------------------------------------
    def _analyze_joins(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        b_ops = self.code.of_type(OperationType.BROADCAST)
        c_ops = self.code.of_type(OperationType.JOIN)

        # Broadcast join analysis
        for b_op in b_ops:
            loc = b_op.location(self.code.source_file)
            target_df = b_op.arguments.get("dataframe") or b_op.dataframe or "right_side"

            if self.volume_gb is None:
                # UNKNOWN runtime evidence
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-JOIN-001:{b_op.line}",
                        rule_id="IMP-JOIN-001",
                        dimension=ImplementationDimension.JOIN_STRATEGY,
                        title="Broadcast Join Explicitly Requested (Volume Unproven)",
                        description=(
                            f"Broadcast join is explicitly requested for dataset '{target_df}'. "
                            "Static evidence does not establish that the dataset remains small enough for safe broadcast."
                        ),
                        status=CheckpointStatus.UNKNOWN,
                        severity=Severity.MEDIUM,
                        observed={"line": b_op.line, "code": b_op.code, "dataframe": target_df},
                        expected={"broadcast_side_volume_gb": "< 0.1 GB or runtime statistics"},
                        evidence=[f"broadcast({target_df}) at line {b_op.line}"],
                        recommendation="Confirm broadcast dataset size via runtime metrics or table statistics.",
                        confidence=0.5,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            elif self.provenance == EvidenceProvenanceKind.RUNTIME or self.provenance == EvidenceProvenanceKind.HISTORICAL_RUN:
                # Runtime evidence available
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-JOIN-002:{b_op.line}",
                        rule_id="IMP-JOIN-002",
                        dimension=ImplementationDimension.JOIN_STRATEGY,
                        title="Broadcast Choice Supported by Runtime Evidence",
                        description=f"Broadcast choice for '{target_df}' is supported by available runtime evidence.",
                        status=CheckpointStatus.PASS,
                        severity=Severity.INFO,
                        observed={"line": b_op.line, "code": b_op.code, "volume_gb": self.volume_gb},
                        expected={"small_broadcast_side": True},
                        evidence=[f"broadcast({target_df}) at line {b_op.line} verified against runtime metrics"],
                        recommendation="Maintain broadcast strategy.",
                        confidence=0.9,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            elif self.volume_gb >= 10.0:
                # High volume static risk
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-JOIN-003:{b_op.line}",
                        rule_id="IMP-JOIN-003",
                        dimension=ImplementationDimension.JOIN_STRATEGY,
                        title="Potentially Dangerous Broadcast Join on Large Volume",
                        description=f"Broadcast join requested on large pipeline input (~{self.volume_gb:.0f} GB).",
                        status=CheckpointStatus.WARN,
                        severity=Severity.HIGH,
                        observed={"line": b_op.line, "code": b_op.code, "volume_gb": self.volume_gb},
                        expected={"broadcast_side_volume_gb": "< 0.1 GB"},
                        evidence=[f"broadcast({target_df}) at line {b_op.line} with input volume {self.volume_gb} GB"],
                        recommendation="Verify that the broadcast side is strictly a small lookup table.",
                        confidence=0.7,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

        # Cross join analysis
        for j_op in c_ops:
            how = str(j_op.arguments.get("how", "")).lower().strip("'\"")
            if how == "cross":
                loc = j_op.location(self.code.source_file)
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-JOIN-004:{j_op.line}",
                        rule_id="IMP-JOIN-004",
                        dimension=ImplementationDimension.JOIN_STRATEGY,
                        title="Cross/Cartesian Join Implemented",
                        description="Developer implemented an explicit cross join operation.",
                        status=CheckpointStatus.WARN if (self.volume_gb and self.volume_gb < 1.0) else CheckpointStatus.FAIL,
                        severity=Severity.MEDIUM if (self.volume_gb and self.volume_gb < 1.0) else Severity.HIGH,
                        observed={"line": j_op.line, "code": j_op.code, "how": how},
                        expected={"equi_join_condition": True},
                        evidence=[f"crossJoin / join(..., how='cross') at line {j_op.line}"],
                        recommendation="Ensure cross join is strictly intended on small datasets; add join conditions where applicable.",
                        confidence=0.85,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

        return findings

    # -------------------------------------------------------------------------
    # 5. Cache / Persist Lifecycle
    # -------------------------------------------------------------------------
    def _analyze_cache_lifecycle(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        caches = self.code.of_type(OperationType.CACHE, OperationType.PERSIST)
        unpersists = [o for o in self.code.operations if o.code and "unpersist" in o.code]

        for c_op in caches:
            loc = c_op.location(self.code.source_file)
            df = c_op.dataframe or "<unknown>"
            uses = flow_mod.downstream_uses(self.code, df, c_op.line) if c_op.dataframe else 0

            # Find matching unpersist
            has_unpersist = any(
                u.line > c_op.line and (u.dataframe == df or df in u.code)
                for u in unpersists
            )

            if uses >= 2 and has_unpersist:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-CACHE-001:{c_op.line}",
                        rule_id="IMP-CACHE-001",
                        dimension=ImplementationDimension.CACHE_LIFECYCLE,
                        title="Optimal Cache Lifecycle Implemented",
                        description=f"Dataset '{df}' is cached, reused by multiple downstream transformations ({uses}x), and explicitly unpersisted.",
                        status=CheckpointStatus.PASS,
                        severity=Severity.INFO,
                        observed={
                            "line": c_op.line,
                            "dataframe": df,
                            "downstream_uses": uses,
                            "unpersisted": True,
                        },
                        expected={"cache_reuse_and_unpersist": True},
                        evidence=[
                            f"cache() at line {c_op.line}, {uses} downstream uses, unpersist() detected after final use"
                        ],
                        recommendation="Maintain good cache lifecycle management.",
                        confidence=0.9,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            elif uses < 2:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-CACHE-002:{c_op.line}",
                        rule_id="IMP-CACHE-002",
                        dimension=ImplementationDimension.CACHE_LIFECYCLE,
                        title="Unproven / Single-Use Cache Implementation",
                        description=f"Dataset '{df}' is cached but only {uses} downstream action/use is statically visible; cache benefit is unproven.",
                        status=CheckpointStatus.WARN,
                        severity=Severity.LOW,
                        observed={
                            "line": c_op.line,
                            "dataframe": df,
                            "downstream_uses": uses,
                            "unpersisted": has_unpersist,
                        },
                        expected={"downstream_uses": ">= 2"},
                        evidence=[f"cache() at line {c_op.line} with {uses} downstream uses"],
                        recommendation="Remove cache() if dataset is evaluated only once.",
                        confidence=0.7,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            elif not has_unpersist:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-CACHE-003:{c_op.line}",
                        rule_id="IMP-CACHE-003",
                        dimension=ImplementationDimension.CACHE_LIFECYCLE,
                        title="Cache Implemented Without Explicit Unpersist",
                        description=f"Dataset '{df}' is cached and reused ({uses}x), but no corresponding unpersist() is statically visible.",
                        status=CheckpointStatus.WARN,
                        severity=Severity.LOW,
                        observed={
                            "line": c_op.line,
                            "dataframe": df,
                            "downstream_uses": uses,
                            "unpersisted": False,
                        },
                        expected={"unpersist_after_use": True},
                        evidence=[f"cache() at line {c_op.line} reused {uses}x without unpersist()"],
                        recommendation="Add df.unpersist() after the final downstream action to release executor memory.",
                        confidence=0.8,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

        return findings

    # -------------------------------------------------------------------------
    # 6. Checkpoint Lifecycle
    # -------------------------------------------------------------------------
    def _analyze_checkpoint_lifecycle(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        checkpoints = [
            o for o in self.code.operations if o.code and ("checkpoint" in o.code or "localCheckpoint" in o.code)
        ]

        for cp_op in checkpoints:
            loc = cp_op.location(self.code.source_file)
            is_local = "localCheckpoint" in cp_op.code
            df = cp_op.dataframe or "<unknown>"
            uses = flow_mod.downstream_uses(self.code, df, cp_op.line) if cp_op.dataframe else 0

            kind = "localCheckpoint()" if is_local else "checkpoint()"

            if uses >= 1:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-CKPT-001:{cp_op.line}",
                        rule_id="IMP-CKPT-001",
                        dimension=ImplementationDimension.CHECKPOINT_LIFECYCLE,
                        title=f"{kind} Implemented with Active Downstream Usage",
                        description=f"Developer implemented {kind} on dataset '{df}' prior to downstream operations ({uses}x).",
                        status=CheckpointStatus.PASS,
                        severity=Severity.INFO,
                        observed={"line": cp_op.line, "dataframe": df, "is_local": is_local, "uses": uses},
                        expected={"checkpoint_materialized_and_used": True},
                        evidence=[f"{kind} at line {cp_op.line} with {uses} downstream uses"],
                        recommendation="Ensure reliable storage/checkpoint directory is configured for non-local checkpoints.",
                        confidence=0.85,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            else:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-CKPT-002:{cp_op.line}",
                        rule_id="IMP-CKPT-002",
                        dimension=ImplementationDimension.CHECKPOINT_LIFECYCLE,
                        title=f"{kind} Implemented Without Visible Reuse",
                        description=f"Developer implemented {kind} on dataset '{df}', but no downstream usage is statically visible.",
                        status=CheckpointStatus.WARN,
                        severity=Severity.LOW,
                        observed={"line": cp_op.line, "dataframe": df, "is_local": is_local, "uses": uses},
                        expected={"downstream_uses": ">= 1"},
                        evidence=[f"{kind} at line {cp_op.line} without downstream uses"],
                        recommendation=f"Verify if {kind} is necessary if dataset is not reused.",
                        confidence=0.7,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

        return findings

    # -------------------------------------------------------------------------
    # 7. Resource & Process Lifecycle
    # -------------------------------------------------------------------------
    def _analyze_resource_lifecycle(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        source_text = self.raw_source

        # Parse AST for resource acquisition / cleanup / context managers
        try:
            tree = ast.parse(source_text, filename=self.code.source_file)
        except Exception:
            return findings

        class ResourceVisitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.context_managers: list[ast.With] = []
                self.explicit_opens: list[tuple[int, str]] = []
                self.explicit_closes: list[tuple[int, str]] = []
                self.try_finallys: list[ast.Try] = []

            def visit_With(self, node: ast.With) -> None:
                self.context_managers.append(node)
                self.generic_visit(node)

            def visit_Call(self, node: ast.Call) -> None:
                func_name = ""
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    func_name = node.func.attr

                if func_name in ("open", "connect", "get_connection", "cursor"):
                    self.explicit_opens.append((node.lineno, func_name))
                elif func_name in ("close", "stop", "disconnect", "commit"):
                    self.explicit_closes.append((node.lineno, func_name))

                self.generic_visit(node)

            def visit_Try(self, node: ast.Try) -> None:
                if node.finalbody:
                    self.try_finallys.append(node)
                self.generic_visit(node)

        visitor = ResourceVisitor()
        visitor.visit(tree)

        # Context manager findings (Positive)
        for with_node in visitor.context_managers:
            loc = f"{self.code.source_file}:{with_node.lineno}"
            findings.append(
                ForensicFinding(
                    finding_id=f"IMP-RES-001:{with_node.lineno}",
                    rule_id="IMP-RES-001",
                    dimension=ImplementationDimension.RESOURCE_LIFECYCLE,
                    title="Resource Context Manager Implemented",
                    description="Resource acquired using Python 'with' context manager, guaranteeing clean acquisition and release.",
                    status=CheckpointStatus.PASS,
                    severity=Severity.INFO,
                    observed={"line": with_node.lineno, "pattern": "with context manager"},
                    expected={"resource_cleanup": "guaranteed via context manager"},
                    evidence=[f"'with' statement at line {with_node.lineno}"],
                    recommendation="Maintain context manager resource pattern.",
                    confidence=0.95,
                    provenance=self.provenance,
                    location=loc,
                )
            )

        # Explicit open/close matching
        for open_line, open_kind in visitor.explicit_opens:
            loc = f"{self.code.source_file}:{open_line}"
            matching_closes = [
                c_line for c_line, _ in visitor.explicit_closes if c_line > open_line
            ]
            in_finally = any(
                t.lineno <= open_line and any(f_node.lineno >= open_line for f_node in t.finalbody)
                for t in visitor.try_finallys
            )

            if matching_closes and in_finally:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-RES-002:{open_line}",
                        rule_id="IMP-RES-002",
                        dimension=ImplementationDimension.RESOURCE_LIFECYCLE,
                        title="Resource Released in Finally Block",
                        description=f"Resource '{open_kind}' acquired and explicitly released inside a try/finally block.",
                        status=CheckpointStatus.PASS,
                        severity=Severity.INFO,
                        observed={"open_line": open_line, "close_line": matching_closes[0], "kind": open_kind},
                        expected={"safe_cleanup": True},
                        evidence=[f"Resource '{open_kind}' at line {open_line} closed at line {matching_closes[0]} in finally"],
                        recommendation="Maintain try/finally resource cleanup pattern.",
                        confidence=0.9,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            elif matching_closes:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-RES-003:{open_line}",
                        rule_id="IMP-RES-003",
                        dimension=ImplementationDimension.RESOURCE_LIFECYCLE,
                        title="Resource Explicitly Closed (Outside Exception-Safe Block)",
                        description=f"Resource '{open_kind}' acquired and explicitly closed, but not enclosed in a try/finally or context manager.",
                        status=CheckpointStatus.WARN,
                        severity=Severity.LOW,
                        observed={"open_line": open_line, "close_line": matching_closes[0], "kind": open_kind},
                        expected={"exception_safe_cleanup": True},
                        evidence=[f"Resource '{open_kind}' at line {open_line} closed at line {matching_closes[0]} outside try/finally"],
                        recommendation="Wrap resource acquisition in 'with' statement or try/finally to ensure cleanup during exceptions.",
                        confidence=0.8,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            else:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-RES-004:{open_line}",
                        rule_id="IMP-RES-004",
                        dimension=ImplementationDimension.RESOURCE_LIFECYCLE,
                        title="Potential Resource Lifecycle Issue (Acquired Without Established Cleanup)",
                        description=f"Potential resource lifecycle issue: resource acquisition '{open_kind}' detected but cleanup is not statically established.",
                        status=CheckpointStatus.WARN,
                        severity=Severity.MEDIUM,
                        observed={"open_line": open_line, "kind": open_kind},
                        expected={"explicit_close_or_context_manager": True},
                        evidence=[f"Resource acquisition '{open_kind}' at line {open_line} without visible close()"],
                        recommendation="Ensure all acquired resources are explicitly closed or managed via context managers.",
                        confidence=0.75,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

        return findings

    # -------------------------------------------------------------------------
    # 8. Exception Safety & Swallowed Exceptions
    # -------------------------------------------------------------------------
    def _analyze_exception_safety(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        source_text = self.raw_source

        try:
            tree = ast.parse(source_text, filename=self.code.source_file)
        except Exception:
            return findings

        class ExceptionVisitor(ast.NodeVisitor):
            def __init__(self, src_file: str) -> None:
                self.src_file = src_file
                self.handlers: list[tuple[ast.ExceptHandler, bool, bool, bool]] = []

            def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
                is_broad = node.type is None or (isinstance(node.type, ast.Name) and node.type.id in ("Exception", "BaseException"))
                body_empty = not node.body or (len(node.body) == 1 and isinstance(node.body[0], (ast.Pass, ast.Constant)))

                has_reraise = False
                has_logging = False
                for stmt in ast.walk(node):
                    if isinstance(stmt, ast.Raise):
                        has_reraise = True
                    elif isinstance(stmt, ast.Call):
                        fn_str = ""
                        if isinstance(stmt.func, ast.Name):
                            fn_str = stmt.func.id
                        elif isinstance(stmt.func, ast.Attribute):
                            fn_str = stmt.func.attr
                        if fn_str in ("error", "exception", "warning", "info", "log", "print"):
                            has_logging = True

                self.handlers.append((node, is_broad, body_empty, has_reraise or has_logging))
                self.generic_visit(node)

        visitor = ExceptionVisitor(self.code.source_file)
        visitor.visit(tree)

        for handler, is_broad, body_empty, handled in visitor.handlers:
            loc = f"{self.code.source_file}:{handler.lineno}"

            if body_empty or (is_broad and not handled):
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-EXC-001:{handler.lineno}",
                        rule_id="IMP-EXC-001",
                        dimension=ImplementationDimension.EXCEPTION_SAFETY,
                        title="Swallowed / Bare Exception Handler Detected",
                        description="Developer implemented an exception block that swallows errors without logging or re-raising.",
                        status=CheckpointStatus.FAIL,
                        severity=Severity.HIGH,
                        observed={"line": handler.lineno, "is_broad": is_broad, "body_empty": body_empty},
                        expected={"exception_propagation_or_logging": True},
                        evidence=[f"Except handler at line {handler.lineno} passes/ignores caught exceptions"],
                        recommendation="Log caught exceptions or re-raise after partial failure handling.",
                        confidence=0.9,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            elif is_broad:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-EXC-002:{handler.lineno}",
                        rule_id="IMP-EXC-002",
                        dimension=ImplementationDimension.EXCEPTION_SAFETY,
                        title="Broad Exception Catch Implemented",
                        description="Broad exception handler catches Exception/BaseException instead of specific error types.",
                        status=CheckpointStatus.WARN,
                        severity=Severity.LOW,
                        observed={"line": handler.lineno, "is_broad": True},
                        expected={"specific_exception_types": True},
                        evidence=[f"Broad except handler at line {handler.lineno}"],
                        recommendation="Catch specific expected exceptions (e.g. AnalysisException, PySparkException) where possible.",
                        confidence=0.8,
                        provenance=self.provenance,
                        location=loc,
                    )
                )
            else:
                findings.append(
                    ForensicFinding(
                        finding_id=f"IMP-EXC-003:{handler.lineno}",
                        rule_id="IMP-EXC-003",
                        dimension=ImplementationDimension.EXCEPTION_SAFETY,
                        title="Specific Exception Handling Implemented",
                        description="Developer implemented specific exception handling with appropriate logging or propagation.",
                        status=CheckpointStatus.PASS,
                        severity=Severity.INFO,
                        observed={"line": handler.lineno, "is_broad": False},
                        expected={"proper_exception_handling": True},
                        evidence=[f"Specific except handler at line {handler.lineno}"],
                        recommendation="Maintain proper exception handling standards.",
                        confidence=0.9,
                        provenance=self.provenance,
                        location=loc,
                    )
                )

        return findings

    # -------------------------------------------------------------------------
    # 9. Implementation Completeness & Process Finalization
    # -------------------------------------------------------------------------
    def _analyze_completeness(self) -> list[ForensicFinding]:
        findings: list[ForensicFinding] = []
        source_text = self.raw_source

        try:
            tree = ast.parse(source_text, filename=self.code.source_file)
        except Exception:
            return findings

        class CompletenessVisitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.todo_comments: list[int] = []
                self.not_implemented_nodes: list[int] = []

            def visit_Raise(self, node: ast.Raise) -> None:
                if node.exc and isinstance(node.exc, ast.Call) and isinstance(node.exc.func, ast.Name):
                    if node.exc.func.id == "NotImplementedError":
                        self.not_implemented_nodes.append(node.lineno)
                elif node.exc and isinstance(node.exc, ast.Name) and node.exc.id == "NotImplementedError":
                    self.not_implemented_nodes.append(node.lineno)
                self.generic_visit(node)

        visitor = CompletenessVisitor()
        visitor.visit(tree)

        # Check for TODO / FIXME in comments or code
        for idx, line in enumerate(source_text.splitlines(), 1):
            if "#" in line:
                comment = line.split("#", 1)[1]
                if "TODO" in comment or "FIXME" in comment or "XXX" in comment:
                    visitor.todo_comments.append(idx)

        for line_num in visitor.not_implemented_nodes:
            loc = f"{self.code.source_file}:{line_num}"
            findings.append(
                ForensicFinding(
                    finding_id=f"IMP-COMP-001:{line_num}",
                    rule_id="IMP-COMP-001",
                    dimension=ImplementationDimension.IMPLEMENTATION_COMPLETENESS,
                    title="NotImplementedError Incomplete Path Implemented",
                    description="Potential incomplete lifecycle based on static control-flow evidence (raise NotImplementedError).",
                    status=CheckpointStatus.WARN,
                    severity=Severity.MEDIUM,
                    observed={"line": line_num},
                    expected={"complete_feature_implementation": True},
                    evidence=[f"raise NotImplementedError at line {line_num}"],
                    recommendation="Complete the feature implementation prior to production deployment.",
                    confidence=0.9,
                    provenance=self.provenance,
                    location=loc,
                )
            )

        for line_num in visitor.todo_comments:
            loc = f"{self.code.source_file}:{line_num}"
            findings.append(
                ForensicFinding(
                    finding_id=f"IMP-COMP-002:{line_num}",
                    rule_id="IMP-COMP-002",
                    dimension=ImplementationDimension.IMPLEMENTATION_COMPLETENESS,
                    title="TODO/FIXME Marker Detected in Implementation",
                    description="Developer left a TODO/FIXME marker indicating incomplete code or pending refactoring.",
                    status=CheckpointStatus.WARN,
                    severity=Severity.LOW,
                    observed={"line": line_num},
                    expected={"resolved_todo_markers": True},
                    evidence=[f"TODO/FIXME marker at line {line_num}"],
                    recommendation="Resolve open TODO/FIXME markers prior to production release.",
                    confidence=0.8,
                    provenance=self.provenance,
                    location=loc,
                )
            )

        return findings
