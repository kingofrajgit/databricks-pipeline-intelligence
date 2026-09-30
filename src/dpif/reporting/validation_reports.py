"""Validation Report Persistence (Phase M5L / Online & Offline Persistence).

Persists complete, machine-readable validation results to files:
- reports/<mode>/<resource-id>/<timestamp>/validation.json
- reports/<mode>/<resource-id>/<timestamp>/validation.md

Guarantees:
1. Zero metric fabrication.
2. Zero secret leakage (tokens, keys, and authorization headers masked).
3. Filesystem-safe timestamp directories preventing collision / overwrite.
4. Architectural parity and consistency across online and offline validations.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpif.flow import PipelineFlowGraph
from dpif.models import Checkpoint, CheckpointStatus, PipelineContract, Score
from dpif.models.alignment import ThreeLayerAlignmentAssessment
from dpif.models.implementation import ImplementationForensicsResult
from dpif.models.rerun import RerunAnalysisResult
from dpif.models.sufficiency import EvidenceSufficiencyAssessment
from dpif.models.synthesis import DecisionRiskSynthesisResult
from dpif.orchestration.online import OnlineValidationResult
from dpif.providers.base import mask_sensitive_credentials, sanitize_job_payload
from dpif.readiness.models import ProductionReadinessAssessment


def format_filesystem_timestamp(dt: datetime | None = None) -> str:
    """Format a datetime into a filesystem-safe string (YYYY-MM-DDTHH-MM-SS)."""
    if dt is None:
        dt = datetime.now(UTC)
    return dt.strftime("%Y-%m-%dT%H-%M-%S")


def sanitize_filename_part(name: str) -> str:
    """Sanitize string to be safe for directory names on all OSes."""
    if not name:
        return "unnamed"
    clean = re.sub(r'[\\/*?:"<>|]', "-", name)
    clean = clean.strip(" .-_")
    return clean or "unnamed"


def get_report_paths(
    base_dir: Path | str,
    mode: str,
    resource_id: str,
    timestamp_str: str | None = None,
) -> tuple[Path, Path]:
    """Calculate and create timestamped directory paths for validation reports.

    Returns:
        (json_path, markdown_path)
    """
    ts = timestamp_str or format_filesystem_timestamp()
    safe_mode = sanitize_filename_part(mode).lower()
    safe_id = sanitize_filename_part(resource_id)

    run_dir = Path(base_dir) / safe_mode / safe_id / ts
    run_dir.mkdir(parents=True, exist_ok=True)

    json_path = run_dir / "validation.json"
    md_path = run_dir / "validation.md"
    return json_path, md_path


def sanitize_dict_credentials(data: Any) -> Any:
    """Recursively mask sensitive credentials and tokens across any JSON structure."""
    if isinstance(data, dict):
        cleaned: dict[str, Any] = {}
        for k, v in data.items():
            k_clean = mask_sensitive_credentials(str(k))
            cleaned[k_clean] = sanitize_dict_credentials(v)
        return sanitize_job_payload(cleaned)
    elif isinstance(data, list):
        return [sanitize_dict_credentials(item) for item in data]
    elif isinstance(data, str):
        return mask_sensitive_credentials(data)
    return data


def _append_flow_graph_section(lines: list[str], flow_graph: PipelineFlowGraph | None) -> None:
    """Append the common pipeline flow graph summary (GAP-001) to a markdown report."""
    lines.append("## Pipeline Flow Graph (GAP-001)")
    lines.append("")
    if flow_graph is None:
        lines.append("No pipeline flow graph available.")
        lines.append("")
        return
    issues = flow_graph.structural_issues()
    lines.append(f"- **Flow Summary**: `{flow_graph.summary()}`")
    lines.append(
        f"- **Nodes**: {len(flow_graph.nodes)} "
        f"({len(flow_graph.source_ids)} sources, {len(flow_graph.target_ids)} targets)"
    )
    lines.append(f"- **Edges**: {len(flow_graph.edges)}")
    if issues:
        lines.append(f"- **Structural Issues**: {len(issues)}")
        for issue in issues:
            lines.append(f"  - `{issue.code}`: {issue.message}")
    else:
        lines.append("- **Structural Issues**: none")
    lines.append("")


def generate_online_markdown_report(
    result: OnlineValidationResult,
    timestamp_str: str,
) -> str:
    """Generate human-readable Markdown validation report for an online run."""
    lines: list[str] = []

    lines.append("# DPIF Validation Report (Online)")
    lines.append("")

    # 1. Summary
    lines.append("## Validation Summary")
    lines.append("")
    lines.append(f"- **Resource Type**: `{result.resource_type.upper()}`")
    lines.append(f"- **Resource ID**: `{result.resource_id}`")
    lines.append(f"- **Pipeline Name**: `{result.pipeline_name}`")
    lines.append(f"- **Validation Timestamp**: `{timestamp_str}`")
    lines.append(f"- **Environment**: `{result.environment}`")
    lines.append(f"- **Quality Score**: **{result.quality_score:.1f}/100** ({result.readiness_label})")
    lines.append(f"- **Confidence**: `{result.confidence}`")
    lines.append(f"- **Decision Sufficiency**: `{'TRUE' if result.decision_sufficiency else 'FALSE'}`")
    lines.append(f"- **Final Decision**: **`{result.final_decision}`**")
    lines.append("")

    # 2. Evidence Summary
    lines.append("## Evidence Summary")
    lines.append("")
    lines.append("| Category | Status | Provenance | Resource / Identifier | Error / Notes |")
    lines.append("|:---|:---:|:---:|:---|:---|")
    for diag in result.evidence_diagnostics:
        res_id = diag.resource_id or "N/A"
        err_note = diag.error_message or "-"
        lines.append(f"| {diag.category.capitalize()} | `{diag.status}` | `{diag.provenance}` | `{res_id}` | {err_note} |")
    lines.append("")

    # 2b. Common Pipeline Flow Graph (GAP-001)
    _append_flow_graph_section(lines, result.flow_graph)

    # 3. Checkpoint Results
    lines.append("## Checkpoint Results (CP-001..CP-024)")
    lines.append("")
    lines.append("| Checkpoint | Name | Status | Severity | Score | Findings |")
    lines.append("|:---|:---|:---:|:---:|:---:|:---:|")
    for cp_id in sorted(result.checkpoints.keys()):
        cp = result.checkpoints[cp_id]
        status_md = f"**`{cp.status.value}`**" if cp.status == CheckpointStatus.FAIL else f"`{cp.status.value}`"
        lines.append(
            f"| `{cp.checkpoint_id}` | {cp.name} | {status_md} | `{cp.severity.value}` | {cp.score:.1f} | {len(cp.findings)} |"
        )
    lines.append("")

    # 4. Developer Implementation Forensics (M5E)
    lines.append("## Developer Implementation Forensics (M5E)")
    lines.append("")
    if result.implementation_forensics:
        m5e = result.implementation_forensics
        lines.append(f"- **Overall Status**: `{m5e.overall_status.value}`")
        lines.append(f"- **Evaluated Dimensions**: {len(m5e.dimensions)}")
        lines.append(f"- **Findings Count**: {len(m5e.all_findings)}")
        if m5e.all_findings:
            lines.append("")
            lines.append("### Key Code Findings")
            for f in m5e.all_findings[:5]:
                title = f.title or f.rule_id
                lines.append(f"- `[{f.severity.value}]` **{title}**: {f.recommendation}")
    else:
        lines.append("No developer implementation forensics available.")
    lines.append("")

    # 5. Rerun & Idempotency Forensics (M5F)
    lines.append("## Rerun / Idempotency Forensics (M5F)")
    lines.append("")
    if result.rerun_analysis:
        m5f = result.rerun_analysis
        lines.append(f"- **Overall Status**: `{m5f.overall_status.value}`")
        lines.append(f"- **Idempotency Status**: `{m5f.idempotency.overall_status.value}`")
        lines.append(f"- **Duplicate Risk**: `{m5f.duplicate_risk.risk_level.value}`")
        lines.append(f"- **Data Loss Risk**: `{m5f.data_loss_risk.risk_level.value}`")
        lines.append(f"- **Total Rerun Findings**: {len(m5f.all_findings)}")
    else:
        lines.append("No rerun/idempotency analysis available.")
    lines.append("")

    # 6. Three-Layer Alignment Forensics (M5G)
    lines.append("## Three-Layer Alignment Forensics (M5G)")
    lines.append("")
    if result.alignment_analysis:
        m5g = result.alignment_analysis
        lines.append(f"- **Overall Status**: `{m5g.overall_status.value}`")
        lines.append(f"- **Drift Severity**: `{m5g.drift_severity.value}`")
        lines.append(f"- **Blocking Drift**: `{'YES' if m5g.has_blocking_drift else 'NO'}`")
        lines.append(f"- **Total Drifts**: {m5g.total_drifts}")
        lines.append(f"- **Unknown Layers**: {m5g.unknown_layers_count}")
    else:
        lines.append("No three-layer alignment analysis available.")
    lines.append("")

    # 7. Evidence Sufficiency (M5H)
    lines.append("## Evidence Sufficiency (M5H)")
    lines.append("")
    if result.evidence_sufficiency:
        m5h = result.evidence_sufficiency
        lines.append(f"- **Coverage Score**: {m5h.coverage_score:.1f}%")
        lines.append(f"- **Confidence Level**: `{m5h.overall_confidence.value}`")
        lines.append(f"- **Decision Sufficiency**: `{'TRUE' if m5h.overall_decision_sufficiency else 'FALSE'}`")
        if m5h.critical_missing_evidence:
            lines.append("- **Critical Missing Evidence**:")
            for m in m5h.critical_missing_evidence:
                lines.append(f"  - {m}")
    else:
        lines.append("No evidence sufficiency assessment available.")
    lines.append("")

    # 8. Production Decision (CP-FINAL & M5I)
    lines.append("## Production Decision (CP-FINAL & M5I)")
    lines.append("")
    lines.append(f"- **Final Decision**: **`{result.final_decision}`**")
    lines.append(f"- **Quality Score**: {result.quality_score:.1f}/100")
    lines.append(f"- **Confidence**: `{result.confidence}`")
    lines.append("")

    if result.decision_risk_synthesis:
        syn = result.decision_risk_synthesis
        if syn.blockers:
            lines.append(f"### Blockers ({len(syn.blockers)})")
            lines.append("")
            for idx, b in enumerate(syn.blockers, 1):
                sev_val = b.severity.value
                lines.append(f"{idx}. `[{sev_val}]` **{b.title}** (Source: `{b.source}`)")
                if b.description:
                    lines.append(f"   - *Description*: {b.description}")
                if b.resolution_requirement:
                    lines.append(f"   - *Resolution*: {b.resolution_requirement}")
            lines.append("")

        if syn.top_risks:
            lines.append(f"### Top Operational & Architectural Risks ({len(syn.top_risks)})")
            lines.append("")
            for idx, r in enumerate(syn.top_risks, 1):
                sev_val = r.severity.value
                cat_val = r.category.value
                lines.append(f"{idx}. `[{sev_val}]` **{r.title}** (`{cat_val}`)")
                if r.consequence:
                    lines.append(f"   - *Consequence*: {r.consequence}")
            lines.append("")

        if syn.risk_chains:
            lines.append("### Causal Risk Chains")
            lines.append("")
            for chain in syn.risk_chains:
                lines.append(f"- **{chain.title}**:")
                lines.append(f"  `{' -> '.join(chain.steps)}`")
            lines.append("")

        if syn.remediations:
            lines.append("### Required Actions")
            lines.append("")
            for rem in syn.remediations:
                lines.append(f"- `[P{rem.priority}]` **{rem.title}**: {rem.description}")
            lines.append("")

    return "\n".join(lines)


def generate_offline_markdown_report(
    contract: PipelineContract,
    checkpoints: dict[str, Checkpoint],
    score: float,
    readiness_label: str,
    impl_assessment: ImplementationForensicsResult | None,
    rerun_assessment: RerunAnalysisResult | None,
    alignment_assessment: ThreeLayerAlignmentAssessment | None,
    sufficiency_assessment: EvidenceSufficiencyAssessment | None,
    assessment: ProductionReadinessAssessment | None,
    synthesis_assessment: DecisionRiskSynthesisResult | None,
    timestamp_str: str,
    flow_graph: PipelineFlowGraph | None = None,
) -> str:
    """Generate human-readable Markdown validation report for an offline run."""
    lines: list[str] = []

    lines.append("# DPIF Validation Report (Offline)")
    lines.append("")

    # 1. Summary
    lines.append("## Validation Summary")
    lines.append("")
    contract_id = getattr(contract, "contract_id", "unnamed")
    pipe_name = getattr(contract, "pipeline_name", contract_id)
    env = getattr(contract, "environment", "production")
    final_decision = (
        synthesis_assessment.final_decision.value
        if synthesis_assessment
        else readiness_label
    )
    conf_str = (
        sufficiency_assessment.overall_confidence.value
        if sufficiency_assessment
        else "UNKNOWN"
    )
    suff_str = (
        "TRUE"
        if (sufficiency_assessment and sufficiency_assessment.overall_decision_sufficiency)
        else "FALSE"
    )

    lines.append(f"- **Contract ID**: `{contract_id}`")
    lines.append(f"- **Pipeline Name**: `{pipe_name}`")
    lines.append(f"- **Validation Timestamp**: `{timestamp_str}`")
    lines.append(f"- **Environment**: `{env}`")
    lines.append(f"- **Quality Score**: **{score:.1f}/100** ({readiness_label})")
    lines.append(f"- **Confidence**: `{conf_str}`")
    lines.append(f"- **Decision Sufficiency**: `{suff_str}`")
    lines.append(f"- **Final Decision**: **`{final_decision}`**")
    lines.append("")

    # 2. Evidence Summary
    lines.append("## Evidence Summary")
    lines.append("")
    lines.append("| Evidence Type | Status | Provenance | Details |")
    lines.append("|:---|:---:|:---:|:---|")
    lines.append(f"| Contract | `STATIC` | `CONTRACT_FILE` | `{contract_id}` |")
    has_source = bool(getattr(contract, "source", None))
    lines.append(f"| Source Definition | `{'DECLARED' if has_source else 'MISSING'}` | `CONTRACT` | `{getattr(contract.source, 'path', 'N/A') if has_source else 'N/A'}` |")
    has_target = bool(getattr(contract, "target", None))
    lines.append(f"| Target Definition | `{'DECLARED' if has_target else 'MISSING'}` | `CONTRACT` | `{getattr(contract.target, 'path', 'N/A') if has_target else 'N/A'}` |")
    lines.append("")

    # 2b. Common Pipeline Flow Graph (GAP-001)
    _append_flow_graph_section(lines, flow_graph)

    # 3. Checkpoint Results
    lines.append("## Checkpoint Results (CP-001..CP-024)")
    lines.append("")
    lines.append("| Checkpoint | Name | Status | Severity | Score | Findings |")
    lines.append("|:---|:---|:---:|:---:|:---:|:---:|")
    for cp_id in sorted(checkpoints.keys()):
        cp = checkpoints[cp_id]
        status_md = f"**`{cp.status.value}`**" if cp.status == CheckpointStatus.FAIL else f"`{cp.status.value}`"
        lines.append(
            f"| `{cp.checkpoint_id}` | {cp.name} | {status_md} | `{cp.severity.value}` | {cp.score:.1f} | {len(cp.findings)} |"
        )
    lines.append("")

    # 4. M5E
    lines.append("## Developer Implementation Forensics (M5E)")
    lines.append("")
    if impl_assessment:
        lines.append(f"- **Overall Status**: `{impl_assessment.overall_status.value}`")
        lines.append(f"- **Evaluated Dimensions**: {len(impl_assessment.dimensions)}")
        lines.append(f"- **Findings Count**: {len(impl_assessment.all_findings)}")
    else:
        lines.append("No developer implementation forensics available.")
    lines.append("")

    # 5. M5F
    lines.append("## Rerun / Idempotency Forensics (M5F)")
    lines.append("")
    if rerun_assessment:
        lines.append(f"- **Overall Status**: `{rerun_assessment.overall_status.value}`")
        lines.append(f"- **Idempotency Status**: `{rerun_assessment.idempotency.overall_status.value}`")
        lines.append(f"- **Duplicate Risk**: `{rerun_assessment.duplicate_risk.risk_level.value}`")
        lines.append(f"- **Data Loss Risk**: `{rerun_assessment.data_loss_risk.risk_level.value}`")
        lines.append(f"- **Total Rerun Findings**: {len(rerun_assessment.all_findings)}")
    else:
        lines.append("No rerun/idempotency analysis available.")
    lines.append("")

    # 6. M5G
    lines.append("## Three-Layer Alignment Forensics (M5G)")
    lines.append("")
    if alignment_assessment:
        lines.append(f"- **Overall Status**: `{alignment_assessment.overall_status.value}`")
        lines.append(f"- **Drift Severity**: `{alignment_assessment.drift_severity.value}`")
        lines.append(f"- **Blocking Drift**: `{'YES' if alignment_assessment.has_blocking_drift else 'NO'}`")
        lines.append(f"- **Total Drifts**: {alignment_assessment.total_drifts}")
    else:
        lines.append("No three-layer alignment analysis available.")
    lines.append("")

    # 7. M5H
    lines.append("## Evidence Sufficiency (M5H)")
    lines.append("")
    if sufficiency_assessment:
        lines.append(f"- **Coverage Score**: {sufficiency_assessment.coverage_score:.1f}%")
        lines.append(f"- **Confidence Level**: `{sufficiency_assessment.overall_confidence.value}`")
        lines.append(f"- **Decision Sufficiency**: `{'TRUE' if sufficiency_assessment.overall_decision_sufficiency else 'FALSE'}`")
        if sufficiency_assessment.critical_missing_evidence:
            lines.append("- **Critical Missing Evidence**:")
            for m in sufficiency_assessment.critical_missing_evidence:
                lines.append(f"  - {m}")
    else:
        lines.append("No evidence sufficiency assessment available.")
    lines.append("")

    # 8. Production Decision (CP-FINAL & M5I)
    lines.append("## Production Decision (CP-FINAL & M5I)")
    lines.append("")
    lines.append(f"- **Final Decision**: **`{final_decision}`**")
    lines.append(f"- **Quality Score**: {score:.1f}/100")
    lines.append(f"- **Confidence**: `{conf_str}`")
    lines.append("")

    if synthesis_assessment:
        if synthesis_assessment.blockers:
            lines.append(f"### Blockers ({len(synthesis_assessment.blockers)})")
            lines.append("")
            for idx, b in enumerate(synthesis_assessment.blockers, 1):
                sev_val = b.severity.value
                lines.append(f"{idx}. `[{sev_val}]` **{b.title}** (Source: `{b.source}`)")
                if b.description:
                    lines.append(f"   - *Description*: {b.description}")
                if b.resolution_requirement:
                    lines.append(f"   - *Resolution*: {b.resolution_requirement}")
            lines.append("")

        if synthesis_assessment.top_risks:
            lines.append(f"### Top Operational & Architectural Risks ({len(synthesis_assessment.top_risks)})")
            lines.append("")
            for idx, r in enumerate(synthesis_assessment.top_risks, 1):
                sev_val = r.severity.value
                cat_val = r.category.value
                lines.append(f"{idx}. `[{sev_val}]` **{r.title}** (`{cat_val}`)")
                if r.consequence:
                    lines.append(f"   - *Consequence*: {r.consequence}")
            lines.append("")

        if synthesis_assessment.remediations:
            lines.append("### Required Actions")
            lines.append("")
            for rem in synthesis_assessment.remediations:
                lines.append(f"- `[P{rem.priority}]` **{rem.title}**: {rem.description}")
            lines.append("")

    return "\n".join(lines)


def persist_online_validation_report(
    result: OnlineValidationResult,
    output_dir: Path | str = "reports",
) -> tuple[Path, Path]:
    """Persist complete online validation result as validation.json and validation.md.

    Returns:
        (json_path, markdown_path)
    """
    ts = format_filesystem_timestamp()
    resource_key = str(result.resource_id or result.pipeline_name or "workload")
    json_path, md_path = get_report_paths(
        base_dir=output_dir,
        mode="online",
        resource_id=resource_key,
        timestamp_str=ts,
    )

    # 1. JSON Payload
    payload = result.to_dict()
    payload["timestamp"] = datetime.now(UTC).isoformat()
    payload["report_paths"] = {
        "json": str(json_path),
        "markdown": str(md_path),
    }
    clean_payload = sanitize_dict_credentials(payload)

    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(clean_payload, jf, indent=2)

    # 2. Markdown Report
    md_content = generate_online_markdown_report(result, timestamp_str=ts)
    clean_md = mask_sensitive_credentials(md_content)

    with open(md_path, "w", encoding="utf-8") as mf:
        mf.write(clean_md)

    return json_path, md_path


def persist_offline_validation_report(
    contract: PipelineContract,
    checkpoints: dict[str, Checkpoint],
    score: float | Score,
    readiness_label: str,
    impl_assessment: ImplementationForensicsResult | None,
    rerun_assessment: RerunAnalysisResult | None,
    alignment_assessment: ThreeLayerAlignmentAssessment | None,
    sufficiency_assessment: EvidenceSufficiencyAssessment | None,
    assessment: ProductionReadinessAssessment | None,
    synthesis_assessment: DecisionRiskSynthesisResult | None,
    output_dir: Path | str = "reports",
    flow_graph: PipelineFlowGraph | None = None,
) -> tuple[Path, Path]:
    """Persist complete offline validation result as validation.json and validation.md.

    Returns:
        (json_path, markdown_path)
    """
    ts = format_filesystem_timestamp()
    contract_key = (
        getattr(contract, "pipeline_name", None)
        or getattr(contract, "contract_id", None)
        or "pipeline"
    )
    json_path, md_path = get_report_paths(
        base_dir=output_dir,
        mode="offline",
        resource_id=str(contract_key),
        timestamp_str=ts,
    )

    num_score = score.overall if isinstance(score, Score) else float(score)

    # 1. JSON Payload
    payload: dict[str, Any] = {
        "execution_mode": "offline",
        "timestamp": datetime.now(UTC).isoformat(),
        "contract_id": getattr(contract, "contract_id", "unnamed"),
        "pipeline_name": getattr(contract, "pipeline_name", "unnamed"),
        "environment": getattr(contract, "environment", "production"),
        "quality_score": round(num_score, 1),
        "readiness_label": readiness_label,
        "confidence": sufficiency_assessment.overall_confidence.value if sufficiency_assessment else "UNKNOWN",
        "decision_sufficiency": sufficiency_assessment.overall_decision_sufficiency if sufficiency_assessment else False,
        "final_decision": (
            synthesis_assessment.final_decision.value
            if synthesis_assessment
            else readiness_label
        ),
        "checkpoints": {
            k: {
                "checkpoint_id": v.checkpoint_id,
                "name": v.name,
                "status": v.status.value,
                "severity": v.severity.value,
                "score": round(v.score, 2),
                "findings_count": len(v.findings),
                "findings": [
                    {
                        "rule_id": f.rule_id,
                        "title": f.title or f.name,
                        "severity": f.severity.value,
                        "status": f.status.value,
                        "recommendation": f.recommendation,
                    }
                    for f in v.findings
                ],
            }
            for k, v in sorted(checkpoints.items())
        },
        "report_paths": {
            "json": str(json_path),
            "markdown": str(md_path),
        },
    }

    if impl_assessment:
        payload["implementation_forensics"] = impl_assessment.to_dict()
    if rerun_assessment:
        payload["rerun_analysis"] = rerun_assessment.to_dict()
    if alignment_assessment:
        payload["alignment_analysis"] = alignment_assessment.to_dict()
    if sufficiency_assessment:
        payload["evidence_sufficiency"] = sufficiency_assessment.to_dict()
    if assessment:
        payload["production_readiness"] = assessment.to_dict()
    if synthesis_assessment:
        payload["decision_risk_synthesis"] = synthesis_assessment.to_dict()
    if flow_graph is not None:
        payload["pipeline_flow_graph"] = flow_graph.to_dict()

    clean_payload = sanitize_dict_credentials(payload)

    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(clean_payload, jf, indent=2)

    # 2. Markdown Report
    md_content = generate_offline_markdown_report(
        contract=contract,
        checkpoints=checkpoints,
        score=num_score,
        readiness_label=readiness_label,
        impl_assessment=impl_assessment,
        rerun_assessment=rerun_assessment,
        alignment_assessment=alignment_assessment,
        sufficiency_assessment=sufficiency_assessment,
        assessment=assessment,
        synthesis_assessment=synthesis_assessment,
        timestamp_str=ts,
        flow_graph=flow_graph,
    )
    clean_md = mask_sensitive_credentials(md_content)

    with open(md_path, "w", encoding="utf-8") as mf:
        mf.write(clean_md)

    return json_path, md_path
