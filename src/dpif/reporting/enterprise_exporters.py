"""Enterprise Exporters for Fleet Validation (Phase M5K).

Generates standardized enterprise integration artifacts:
1. JUnit XML for CI/CD test reporting dashboards (GitHub Actions, Azure DevOps, GitLab CI).
2. OASIS SARIF v2.1.0 for security and static analysis code scanning tabs.
3. Markdown PR Summary for executive and engineering release reviews.
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from dpif.models import CheckpointStatus, Severity
from dpif.models.fleet import CollisionStatus, FleetValidationResult
from dpif.providers.base import mask_sensitive_credentials


def export_junit_xml(
    fleet_result: FleetValidationResult,
    output_path: Path | None = None,
) -> str:
    """Generate JUnit-compatible XML representing fleet validation outcomes."""
    testsuites_el = ET.Element(
        "testsuites",
        attrib={
            "name": f"DPIF Fleet Validation - {fleet_result.fleet_name}",
            "tests": str(fleet_result.summary.total_pipelines),
            "failures": str(fleet_result.summary.blocked_policy),
            "errors": str(fleet_result.summary.failed_validations),
            "time": f"{fleet_result.duration_seconds:.3f}",
        },
    )

    for pid, exec_obj in fleet_result.pipeline_executions.items():
        val_res = exec_obj.validation_result
        total_cases = 1
        errors = 1 if not exec_obj.success else 0

        checkpoints_map = getattr(val_res, "checkpoints", {}) if val_res else {}
        total_cases += len(checkpoints_map)

        suite_el = ET.SubElement(
            testsuites_el,
            "testsuite",
            attrib={
                "name": f"pipeline.{pid}",
                "tests": str(total_cases),
                "failures": "1" if not exec_obj.policy_passed else "0",
                "errors": str(errors),
                "time": f"{exec_obj.duration_seconds:.3f}",
            },
        )

        # 1. Policy Gate testcase
        tc_policy = ET.SubElement(
            suite_el,
            "testcase",
            attrib={
                "classname": f"{fleet_result.fleet_name}.{pid}",
                "name": "environment_policy_gate",
                "time": f"{exec_obj.duration_seconds:.3f}",
            },
        )
        if not exec_obj.success:
            err_el = ET.SubElement(
                tc_policy,
                "error",
                attrib={"message": "Pipeline validation execution failure"},
            )
            err_el.text = exec_obj.error_message or "Unknown execution error"
        elif not exec_obj.policy_passed:
            fail_el = ET.SubElement(
                tc_policy,
                "failure",
                attrib={"message": f"Environment policy '{fleet_result.environment.value}' failed"},
            )
            fail_el.text = "\n".join(exec_obj.policy_violations)

        # 2. Checkpoints testcases
        if checkpoints_map:
            for cp_id, cp in checkpoints_map.items():
                cp_name = getattr(cp, "name", cp_id)
                cp_status = getattr(cp, "status", CheckpointStatus.UNKNOWN)
                tc_cp = ET.SubElement(
                    suite_el,
                    "testcase",
                    attrib={
                        "classname": f"{fleet_result.fleet_name}.{pid}.checkpoints",
                        "name": f"{cp_id}_{cp_name.replace(' ', '_')}",
                        "time": "0.000",
                    },
                )
                if cp_status == CheckpointStatus.FAIL:
                    c_fail = ET.SubElement(
                        tc_cp,
                        "failure",
                        attrib={"message": f"Checkpoint {cp_id} failed with score {getattr(cp, 'score', 0.0):.1f}"},
                    )
                    findings = getattr(cp, "findings", [])
                    c_fail.text = "\n".join(
                        f"[{f.severity.value}] {getattr(f, 'title', f.name)}: {getattr(f, 'message', '')}"
                        for f in findings
                    )
                elif cp_status == CheckpointStatus.UNKNOWN:
                    ET.SubElement(
                        tc_cp,
                        "skipped",
                        attrib={"message": "Evidence unavailable (UNKNOWN) - not evaluated"},
                    )

    xml_str = ET.tostring(testsuites_el, encoding="utf-8", xml_declaration=True).decode("utf-8")
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(xml_str, encoding="utf-8")
    return xml_str


def export_sarif(
    fleet_result: FleetValidationResult,
    output_path: Path | None = None,
) -> str:
    """Generate valid OASIS SARIF v2.1.0 for enterprise security & code scanning."""
    rules_dict: dict[str, dict[str, Any]] = {}
    sarif_results: list[dict[str, Any]] = []

    def _severity_to_sarif(sev: Severity | str) -> str:
        s = str(getattr(sev, "value", sev)).upper()
        if s in ("CRITICAL", "HIGH"):
            return "error"
        if s == "MEDIUM":
            return "warning"
        return "note"

    # 1. Pipeline Checkpoints findings
    for pid, exec_obj in fleet_result.pipeline_executions.items():
        val_res = exec_obj.validation_result
        if not val_res:
            continue

        findings = getattr(val_res, "findings", [])
        for f in findings:
            rule_id = getattr(f, "rule_id", "UNKNOWN-RULE")
            title = getattr(f, "title", getattr(f, "name", rule_id))
            message = getattr(f, "message", title)
            sev = getattr(f, "severity", Severity.MEDIUM)

            if rule_id not in rules_dict:
                rules_dict[rule_id] = {
                    "id": rule_id,
                    "name": title,
                    "shortDescription": {"text": title},
                    "defaultConfiguration": {"level": _severity_to_sarif(sev)},
                }

            result_entry: dict[str, Any] = {
                "ruleId": rule_id,
                "level": _severity_to_sarif(sev),
                "message": {"text": f"[{pid}] {message}"},
                "properties": {
                    "pipeline_id": pid,
                    "severity": getattr(sev, "value", str(sev)),
                },
            }

            # If location available from code analysis
            file_path = getattr(f, "file_path", None)
            line_num = getattr(f, "line_number", None)
            if file_path:
                result_entry["locations"] = [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": str(file_path)},
                            "region": {"startLine": int(line_num) if line_num else 1},
                        }
                    }
                ]
            sarif_results.append(result_entry)

    # 2. Cross-Pipeline Collisions
    for col in fleet_result.collisions:
        rule_id = f"COLLISION-{col.target_resource.replace('/', '_').replace(':', '_')}"
        if rule_id not in rules_dict:
            rules_dict[rule_id] = {
                "id": rule_id,
                "name": "Cross-Pipeline Target Collision",
                "shortDescription": {"text": "Multiple pipelines targeting identical resource with conflicting writes"},
                "defaultConfiguration": {"level": "error" if col.status == CollisionStatus.CONFIRMED else "warning"},
            }
        sarif_results.append({
            "ruleId": rule_id,
            "level": "error" if col.status == CollisionStatus.CONFIRMED else "warning",
            "message": {"text": col.description},
            "properties": {
                "status": col.status.value,
                "target_resource": col.target_resource,
                "conflicting_pipelines": col.conflicting_pipeline_ids,
            },
        })

    sarif_doc: dict[str, Any] = {
        "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "Databricks Pipeline Intelligence Framework (DPIF)",
                        "version": "0.1.0",
                        "informationUri": "https://github.com/kingofrajgit/databricks-pipeline-intelligence",
                        "rules": list(rules_dict.values()),
                    }
                },
                "results": sarif_results,
            }
        ],
    }

    sarif_str = json.dumps(sarif_doc, indent=2)
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(sarif_str, encoding="utf-8")
    return sarif_str


def export_markdown_summary(
    fleet_result: FleetValidationResult,
    output_path: Path | None = None,
) -> str:
    """Generate a clean executive & engineering PR comment markdown summary."""
    status_emoji = "✅" if fleet_result.policy_passed else "❌"
    gate_decision = "PASSED POLICY GATE" if fleet_result.policy_passed else "POLICY GATE BLOCKED"

    md_lines: list[str] = [
        f"# {status_emoji} DPIF Enterprise Fleet Validation Report",
        "",
        f"**Fleet**: `{fleet_result.fleet_name}` | **Environment**: `{fleet_result.environment.value.upper()}` | **Gate Decision**: **{gate_decision}**",
        "",
        "## Fleet Summary Metrics",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| **Total Pipelines** | {fleet_result.summary.total_pipelines} |",
        f"| **Validated Successfully** | {fleet_result.summary.successful_validations} |",
        f"| **Execution Failures** | {fleet_result.summary.failed_validations} |",
        f"| **Passed Policy Gate** | {fleet_result.summary.passed_policy} |",
        f"| **Blocked by Policy** | {fleet_result.summary.blocked_policy} |",
        f"| **Fleet Quality Score** | **{fleet_result.summary.fleet_quality_score:.1f} / 100** |",
        f"| **Total Production Blockers** | {fleet_result.summary.total_blockers} |",
        f"| **P0 Risks Identified** | {fleet_result.summary.total_p0_risks} |",
        f"| **Execution Duration** | {fleet_result.duration_seconds:.2f}s |",
        "",
    ]

    # Collisions section
    if fleet_result.collisions:
        md_lines.extend([
            "## ⚠️ Cross-Pipeline Target Collisions",
            "",
            "| Status | Target Resource | Conflicting Pipelines | Description |",
            "|---|---|---|---|",
        ])
        for c in fleet_result.collisions:
            pids_str = ", ".join(f"`{pid}`" for pid in c.conflicting_pipeline_ids)
            md_lines.append(
                f"| **{c.status.value}** | `{c.target_resource}` | {pids_str} | {c.description} |"
            )
        md_lines.append("")

    # Pipeline Breakdown Table
    md_lines.extend([
        "## Pipeline Results Breakdown",
        "",
        "| Pipeline ID | Status | Quality Score | Confidence | Decision | Policy Gate | Details / Violations |",
        "|---|---|---|---|---|---|---|",
    ])

    for pid, exec_obj in fleet_result.pipeline_executions.items():
        vr = exec_obj.validation_result
        if not exec_obj.success or not vr:
            md_lines.append(
                f"| `{pid}` | ❌ FAILED | - | - | - | ❌ BLOCKED | {exec_obj.error_message or 'Execution failure'} |"
            )
            continue

        p_status = "✅ PASS" if exec_obj.policy_passed else "❌ BLOCKED"
        q_score = f"{vr.quality_score:.1f}"
        conf = str(vr.confidence)
        final_dec = str(vr.final_decision)
        violations = "<br>".join(exec_obj.policy_violations) if exec_obj.policy_violations else "Meets all policy criteria"

        md_lines.append(
            f"| `{pid}` | ✅ COMPLETE | {q_score} | `{conf}` | `{final_dec}` | {p_status} | {violations} |"
        )

    md_lines.append("")

    # Prioritized Remediation Actions
    remediations: list[str] = []
    for pid, exec_obj in fleet_result.pipeline_executions.items():
        vr = exec_obj.validation_result
        if vr and hasattr(vr, "decision_risk_synthesis") and vr.decision_risk_synthesis:
            syn = vr.decision_risk_synthesis
            rem_list = []
            if hasattr(syn, "remediations") and isinstance(syn.remediations, list):
                rem_list = syn.remediations
            elif hasattr(syn, "remediation_plans") and isinstance(syn.remediation_plans, list):
                rem_list = syn.remediation_plans
            for rem in rem_list:
                priority = getattr(rem, "priority", "P2")
                desc = getattr(rem, "description", getattr(rem, "title", ""))
                if str(priority) in ("P0", "P1"):
                    remediations.append(f"- **[{priority}] `{pid}`**: {desc}")

    if remediations:
        md_lines.extend([
            "## 🛠️ Prioritized Remediation Actions",
            "",
            *remediations[:10],
            "",
        ])

    md_text = mask_sensitive_credentials("\n".join(md_lines))
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(md_text, encoding="utf-8")
    return md_text
