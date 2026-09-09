"""Cross-domain risk synthesizer and configuration drift analyzer for CP-FINAL.

Correlates findings across Source, Data, Code, SQL, Cluster, Runtime, and Scalability
without re-parsing code or re-computing raw telemetry.
"""

from __future__ import annotations

from typing import Any

from dpif.models import Checkpoint, DataProfile, PipelineContract
from dpif.readiness.models import (
    CrossDomainRisk,
    ExpectedVsImplementedVsActual,
)


def synthesize_cross_domain_risks(
    checkpoints: dict[str, Checkpoint],
    contract: PipelineContract | None = None,
    profile: DataProfile | None = None,
    cluster_config: dict[str, Any] | None = None,
    runtime_data: Any | None = None,
) -> list[CrossDomainRisk]:
    """Synthesize cross-domain risks across intelligence domains."""
    all_findings = [f for cp in checkpoints.values() for f in cp.findings]
    rule_ids = {f.rule_id for f in all_findings}

    risks: list[CrossDomainRisk] = []
    vol = 0.0
    if profile:
        vol = profile.total_gb
    elif contract and contract.source:
        vol = contract.source.expected_volume_gb or 0.0

    # 1. Driver Scalability Risk: Code collect + Large volume
    has_collect = any(
        r in rule_ids for r in ("CODE-PYSPARK-006", "CODE-PYSPARK-007", "SCALABILITY-006")
    )
    if has_collect and vol >= 50.0:
        risks.append(
            CrossDomainRisk(
                risk_id="XDOM-001",
                title="Driver Memory Exhaustion Risk at Scale",
                severity="CRITICAL" if vol >= 200.0 else "HIGH",
                contributing_domains=["Code", "Data", "Scalability"],
                evidence_sources=["AST Code Analysis", "Data Profile Metadata"],
                description=(
                    f"Driver-side materialization (`collect`/`toPandas`) detected in pipeline code "
                    f"combined with a substantial data workload ({vol:.1f} GB). Materializing "
                    "distributed datasets to the Spark driver causes out-of-memory driver crashes."
                ),
                recommendation=(
                    "Replace `collect()` and `toPandas()` with distributed DataFrame actions "
                    "or write transformations directly to Delta lake."
                ),
                confidence=0.95,
            )
        )

    # 2. Cartesian Cross-Join Scaling Risk
    has_cross = any(
        r in rule_ids for r in ("CODE-PYSPARK-005", "CODE-SQL-002", "SCALABILITY-008")
    )
    if has_cross and vol >= 10.0:
        risks.append(
            CrossDomainRisk(
                risk_id="XDOM-002",
                title="Cartesian Product Scaling Risk",
                severity="CRITICAL" if vol >= 50.0 else "HIGH",
                contributing_domains=["Code", "SQL", "Data"],
                evidence_sources=["AST / SQL Parser", "Data Profile"],
                description=(
                    f"Cartesian cross join detected on a non-trivial workload ({vol:.1f} GB). "
                    "Unconstrained cross joins generate O(N*M) record explosions at scale, "
                    "saturating executor disk and network."
                ),
                recommendation=(
                    "Eliminate Cartesian joins: supply explicit join conditions with ON clauses "
                    "or apply strict pre-filtering."
                ),
                confidence=0.95,
            )
        )

    # 3. Network Shuffle Explosion
    has_high_shuffle = any(
        r in rule_ids for r in ("RUNTIME-PERF-002", "RUNTIME-PERF-003", "SCALABILITY-007")
    )
    if has_high_shuffle:
        risks.append(
            CrossDomainRisk(
                risk_id="XDOM-003",
                title="High Network Shuffle & Spill Risk",
                severity="HIGH",
                contributing_domains=["Performance", "Scalability"],
                evidence_sources=["Runtime Telemetry", "Linear Projections"],
                description=(
                    "Observed or projected shuffle volume exceeds safe network thresholds, "
                    "causing executor memory spill to disk and stage stragglers."
                ),
                recommendation=(
                    "Enable Adaptive Query Execution (AQE), co-partition datasets on join keys, "
                    "and avoid wide transformations before filtering."
                ),
                confidence=0.85,
            )
        )

    # 4. Cluster Autoscaling & Peak Headroom Constraint
    has_peak_risk = any(r in rule_ids for r in ("SCALABILITY-002", "SCALABILITY-011"))
    if has_peak_risk:
        risks.append(
            CrossDomainRisk(
                risk_id="XDOM-004",
                title="Peak Workload Capacity Headroom Risk",
                severity="MEDIUM",
                contributing_domains=["Cluster", "Scalability", "Contract"],
                evidence_sources=["Cluster Config", "Contract Declarations"],
                description=(
                    "Contractual peak ingestion burst exceeds baseline workload capacity on "
                    "a fixed or constrained cluster configuration without empirical verification."
                ),
                recommendation=(
                    "Enable cluster autoscaling or validate cluster worker memory sizing "
                    "against peak ingestion bursts."
                ),
                confidence=0.80,
            )
        )

    # 5. Small File Proliferation under Workload Growth
    has_small_files = any(r in rule_ids for r in ("DATA-001", "SCALABILITY-004"))
    if has_small_files:
        risks.append(
            CrossDomainRisk(
                risk_id="XDOM-005",
                title="Small File Proliferation & Metastore Pressure",
                severity="MEDIUM",
                contributing_domains=["Data", "Scalability"],
                evidence_sources=["Data Profile Metadata", "File Projections"],
                description=(
                    "Average file size is small and projected growth expands file counts beyond "
                    "safe catalog listing limits, creating driver metadata bottlenecks."
                ),
                recommendation=(
                    "Enable Delta Auto Compaction and schedule regular `OPTIMIZE` commands."
                ),
                confidence=0.85,
            )
        )

    return risks


def compare_expected_implemented_actual(
    contract: PipelineContract | None,
    cluster_config: dict[str, Any] | None,
    job_config: dict[str, Any] | None,
    profile: DataProfile | None,
    runtime_data: Any | None,
    actual_environment: dict[str, Any] | None = None,
) -> list[ExpectedVsImplementedVsActual]:
    """Compare Expected (Contract) vs Implemented (Config) vs Actual (Databricks/Runtime)."""
    comparisons: list[ExpectedVsImplementedVsActual] = []

    exp_cluster = getattr(contract, "_cluster_raw", {}) if contract else {}
    if not isinstance(exp_cluster, dict):
        exp_cluster = {}

    # 1. Runtime / Spark Version (DBR)
    exp_dbr = exp_cluster.get("spark_version")
    impl_dbr = cluster_config.get("spark_version") if cluster_config else None
    act_dbr = actual_environment.get("spark_version") if actual_environment else None
    dbr_drift = False
    drift_sev = "NONE"
    notes = ""
    if act_dbr and exp_dbr and str(act_dbr) != str(exp_dbr):
        dbr_drift = True
        drift_sev = "BLOCKING"
        notes = f"Actual cluster runtime ({act_dbr}) does not match expected contract ({exp_dbr})"
    elif impl_dbr and exp_dbr and str(impl_dbr) != str(exp_dbr):
        dbr_drift = True
        drift_sev = "WARN"
        notes = (
            f"Implemented cluster config ({impl_dbr}) differs from expected contract ({exp_dbr})"
        )

    comparisons.append(
        ExpectedVsImplementedVsActual(
            parameter="Spark/DBR Version",
            domain="Cluster",
            expected=exp_dbr,
            implemented=impl_dbr,
            actual=act_dbr,
            is_drift=dbr_drift,
            drift_severity=drift_sev,
            notes=notes,
        )
    )

    # 2. Worker Node Type
    exp_node = exp_cluster.get("node_type_id") or exp_cluster.get("node_type")
    impl_node = cluster_config.get("node_type_id") if cluster_config else None
    act_node = actual_environment.get("node_type_id") if actual_environment else None
    node_drift = False
    if act_node and exp_node and str(act_node) != str(exp_node):
        node_drift = True
    comparisons.append(
        ExpectedVsImplementedVsActual(
            parameter="Worker Node Type",
            domain="Cluster",
            expected=exp_node,
            implemented=impl_node,
            actual=act_node,
            is_drift=node_drift,
            drift_severity="WARN" if node_drift else "NONE",
            notes="Node type drift detected" if node_drift else "",
        )
    )

    # 3. Worker Count / Autoscaling
    exp_workers = exp_cluster.get("num_workers")
    impl_workers = cluster_config.get("num_workers") if cluster_config else None
    act_workers = actual_environment.get("num_workers") if actual_environment else None
    worker_drift = False
    if act_workers is not None and exp_workers is not None and act_workers != exp_workers:
        worker_drift = True
    comparisons.append(
        ExpectedVsImplementedVsActual(
            parameter="Worker Count",
            domain="Cluster",
            expected=exp_workers,
            implemented=impl_workers,
            actual=act_workers,
            is_drift=worker_drift,
            drift_severity="WARN" if worker_drift else "NONE",
            notes="Worker count drift detected" if worker_drift else "",
        )
    )

    # 4. Ingestion Volume
    exp_vol = getattr(contract.source, "expected_volume_gb", None) if contract else None
    obs_vol = profile.total_gb if profile else None
    act_vol = None
    if runtime_data:
        act_vol = getattr(runtime_data, "total_input_gb", None)
        if act_vol is None and hasattr(runtime_data, "total_input_bytes"):
            act_vol = round(runtime_data.total_input_bytes / (1024.0**3), 2)
    comparisons.append(
        ExpectedVsImplementedVsActual(
            parameter="Daily Volume (GB)",
            domain="Data",
            expected=exp_vol,
            implemented=obs_vol,
            actual=act_vol,
            is_drift=False,
            drift_severity="NONE",
            notes="Volume observed from telemetry/profile",
        )
    )

    # 5. SLA Runtime Threshold
    exp_sla = (
        getattr(contract.sla, "max_runtime_minutes", None)
        if (contract and contract.sla)
        else None
    )
    act_dur = None
    if runtime_data:
        dur_s = getattr(runtime_data, "duration_seconds", None)
        if dur_s is not None:
            act_dur = round(dur_s / 60.0, 1)
    comparisons.append(
        ExpectedVsImplementedVsActual(
            parameter="Max Duration (Minutes)",
            domain="SLA",
            expected=exp_sla,
            implemented=job_config.get("timeout_seconds", 0) / 60.0 if job_config else None,
            actual=act_dur,
            is_drift=bool(exp_sla and act_dur and act_dur > exp_sla),
            drift_severity="BLOCKING" if (exp_sla and act_dur and act_dur > exp_sla) else "NONE",
            notes=(
                f"Observed runtime ({act_dur}m) exceeded SLA ({exp_sla}m)"
                if (exp_sla and act_dur and act_dur > exp_sla)
                else ""
            ),
        )
    )

    return comparisons
