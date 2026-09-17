"""Enterprise Fleet Validation Orchestrator (Phase M5K).

Orchestrates multi-pipeline validation runs across an enterprise fleet:
- Validates YAML fleet manifests with strict credential segregation
- Executes each pipeline target via OnlineValidationOrchestrator with full failure isolation
- Enforces bounded concurrency (default max_workers=4)
- Detects cross-pipeline resource collisions (e.g. shared Delta table write conflicts)
- Evaluates environment tier policies (development, staging, production)
- Aggregates fleet-level metrics and scores without hiding critical blockers
"""

from __future__ import annotations

import concurrent.futures
import logging
import re
import time
from pathlib import Path
from typing import Any

import yaml

from dpif.config import resolve_databricks_credentials
from dpif.connectors.base import DatabricksConnector
from dpif.connectors.live import DatabricksAPIError, DatabricksApiError, LiveDatabricksConnector
from dpif.error_handling import ConfigurationError
from dpif.models import Severity
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
from dpif.orchestration.online import OnlineValidationOrchestrator
from dpif.providers.base import DatabricksEvidenceProvider

logger = logging.getLogger(__name__)

# Patterns that detect sensitive credentials in manifest files
_SECRET_KEY_PATTERN = re.compile(
    r"(?i)^(token|password|passwd|secret|api_key|apikey|access_token|auth|authorization)$"
)
_SECRET_VALUE_PATTERN = re.compile(
    r"(?i)(dapi[a-f0-9]{32,}|bearer\s+[a-zA-Z0-9_\-\.=]+|ghp_[a-zA-Z0-9]+)"
)


def _scan_for_secrets(obj: Any, path: str = "") -> None:
    """Recursively inspect manifest structure and reject any embedded secrets."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            k_str = str(k)
            if _SECRET_KEY_PATTERN.match(k_str):
                raise ConfigurationError(
                    f"Manifest contains sensitive credential key '{k_str}' at '{path}'. "
                    "Credentials must not be stored in manifests. Provide them via environment variables or CLI."
                )
            if isinstance(v, str) and _SECRET_VALUE_PATTERN.search(v):
                raise ConfigurationError(
                    f"Manifest contains a sensitive credential value at '{path}.{k_str}'. "
                    "Credentials must not be stored in manifests."
                )
            _scan_for_secrets(v, f"{path}.{k_str}" if path else k_str)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, str) and _SECRET_VALUE_PATTERN.search(item):
                raise ConfigurationError(
                    f"Manifest contains a sensitive credential value at '{path}[{i}]'."
                )
            _scan_for_secrets(item, f"{path}[{i}]")


def load_and_validate_fleet_manifest(manifest_path: Path | str) -> FleetManifest:
    """Load and strictly validate an enterprise fleet manifest YAML file.

    Raises:
        FileNotFoundError: If the manifest path does not exist.
        ConfigurationError: If manifest syntax or credentials violations occur.
        ValueError: If manifest schema or target uniqueness violations occur.
    """
    path = Path(manifest_path)
    if not path.exists():
        raise FileNotFoundError(f"Fleet manifest file not found: {path}")

    try:
        raw_text = path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw_text) or {}
    except Exception as exc:
        raise ConfigurationError(f"Malformed YAML in fleet manifest '{path}': {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigurationError(f"Fleet manifest must be a YAML mapping, got {type(data).__name__}")

    # Security check: zero credentials permitted in manifest
    _scan_for_secrets(data)

    fleet_meta = data.get("fleet", {})
    if not isinstance(fleet_meta, dict):
        raise ConfigurationError("'fleet' section must be a dictionary")

    fleet_name = fleet_meta.get("name")
    if not fleet_name or not str(fleet_name).strip():
        raise ValueError("Manifest 'fleet.name' is required and must not be empty")
    fleet_name = str(fleet_name).strip()

    env_raw = fleet_meta.get("environment", "production")
    try:
        fleet_env = EnvironmentTier(str(env_raw).lower())
    except ValueError:
        valid_tiers = ", ".join(t.value for t in EnvironmentTier)
        raise ValueError(f"Invalid fleet environment '{env_raw}'. Must be one of: {valid_tiers}") from None

    workspace_meta = data.get("workspace", {})
    default_workspace = None
    if isinstance(workspace_meta, dict):
        default_workspace = workspace_meta.get("host")

    if "pipelines" not in data:
        raise ValueError("Manifest missing required 'pipelines' section")

    raw_pipelines = data.get("pipelines")
    if not isinstance(raw_pipelines, list):
        raise ConfigurationError("'pipelines' must be a list of pipeline target definitions")
    if len(raw_pipelines) == 0:
        raise ValueError("Manifest 'pipelines' must contain at least one pipeline target")

    targets: list[EnterprisePipelineTarget] = []
    seen_ids: set[str] = set()

    for idx, raw_target in enumerate(raw_pipelines):
        if not isinstance(raw_target, dict):
            raise ConfigurationError(f"Pipeline entry #{idx + 1} must be a dictionary")

        target_id = raw_target.get("id")
        if not target_id or not str(target_id).strip():
            raise ValueError(f"Pipeline target #{idx + 1} missing required 'id' field")

        target_id = str(target_id).strip()
        if target_id in seen_ids:
            raise ValueError(f"Duplicate pipeline id '{target_id}' found in fleet manifest")
        seen_ids.add(target_id)

        job_id = raw_target.get("job_id")
        if job_id is not None:
            try:
                job_id = int(job_id)
            except (ValueError, TypeError):
                raise ValueError(f"Pipeline '{target_id}' has invalid job_id: '{job_id}' (must be an integer)") from None

        dlt_pipeline_id = raw_target.get("pipeline_id")
        if dlt_pipeline_id is not None:
            dlt_pipeline_id = str(dlt_pipeline_id).strip()

        # Contract and code paths
        contract_path_raw = raw_target.get("contract") or raw_target.get("contract_path")
        contract_path: Path | None = None
        if contract_path_raw:
            cp = Path(contract_path_raw)
            contract_path = cp if cp.is_absolute() else (path.parent / cp)
            if not contract_path.exists():
                raise FileNotFoundError(f"Pipeline '{target_id}' referenced contract not found: {contract_path}")

        code_path_raw = raw_target.get("code") or raw_target.get("code_path")
        code_path: Path | None = None
        if code_path_raw:
            cdp = Path(code_path_raw)
            code_path = cdp if cdp.is_absolute() else (path.parent / cdp)
            if not code_path.exists():
                raise FileNotFoundError(f"Pipeline '{target_id}' referenced code not found: {code_path}")

        target_env = raw_target.get("environment")
        target_ws = raw_target.get("workspace") or default_workspace

        targets.append(
            EnterprisePipelineTarget(
                id=target_id,
                job_id=job_id,
                pipeline_id=dlt_pipeline_id,
                contract_path=contract_path,
                code_path=code_path,
                environment=target_env,
                workspace=target_ws,
            )
        )

    return FleetManifest(
        name=fleet_name,
        environment=fleet_env,
        workspace_host=default_workspace,
        pipelines=targets,
    )


def detect_fleet_collisions(
    executions: dict[str, PipelineFleetExecution],
) -> list[CrossPipelineCollisionFinding]:
    """Detect cross-pipeline resource collisions and uncoordinated write contention across the fleet."""
    target_to_pipelines: dict[str, list[tuple[str, str]]] = {}

    for pid, exec_obj in executions.items():
        vr = exec_obj.validation_result
        if not vr or not exec_obj.success:
            continue

        target_resource: str | None = None
        write_mode = "unknown"

        # 1. From rerun analysis
        if hasattr(vr, "rerun_analysis") and vr.rerun_analysis:
            ra = vr.rerun_analysis
            write_mode = getattr(ra, "write_mode", "unknown")
            target_resource = getattr(ra, "target_table", None) or getattr(ra, "target_path", None)

        # 2. From normalized evidence or contract if not yet extracted
        if not target_resource and hasattr(vr, "normalized_evidence") and vr.normalized_evidence:
            ne = vr.normalized_evidence
            if getattr(ne, "dlt_pipeline", None):
                target_resource = getattr(ne.dlt_pipeline, "target_table", None) or getattr(ne.dlt_pipeline, "storage", None)

        if target_resource and str(target_resource).strip():
            norm_key = str(target_resource).strip().lower()
            if norm_key not in target_to_pipelines:
                target_to_pipelines[norm_key] = []
            target_to_pipelines[norm_key].append((pid, str(write_mode).lower()))

    findings: list[CrossPipelineCollisionFinding] = []

    for resource_key, p_list in target_to_pipelines.items():
        if len(p_list) > 1:
            pids = [p[0] for p in p_list]
            write_modes = {p[0]: p[1] for p in p_list}
            distinct_modes = set(write_modes.values())

            # Determine collision certainty & severity
            if any(m in ("overwrite", "replace") for m in distinct_modes):
                status = CollisionStatus.CONFIRMED
                severity = Severity.CRITICAL
                desc = (
                    f"Multiple pipelines ({', '.join(pids)}) target identical resource '{resource_key}' "
                    f"with destructive write mode ({distinct_modes}), risking silent data loss or race conditions."
                )
            elif all(m in ("append",) for m in distinct_modes):
                status = CollisionStatus.POTENTIAL
                severity = Severity.HIGH
                desc = (
                    f"Multiple pipelines ({', '.join(pids)}) append concurrently to identical target '{resource_key}' "
                    "without coordinated transactional locking."
                )
            else:
                status = CollisionStatus.UNKNOWN
                severity = Severity.MEDIUM
                desc = (
                    f"Multiple pipelines ({', '.join(pids)}) reference identical resource '{resource_key}', "
                    "but write operations could not be conclusively determined from available evidence."
                )

            findings.append(
                CrossPipelineCollisionFinding(
                    status=status,
                    severity=severity,
                    target_resource=resource_key,
                    conflicting_pipeline_ids=pids,
                    write_modes=write_modes,
                    description=desc,
                    recommendation=(
                        "Isolate pipeline output targets into distinct partition paths/tables or "
                        "introduce orchestrator-level dependency ordering."
                    ),
                )
            )

    return findings


def aggregate_fleet_metrics(
    executions: dict[str, PipelineFleetExecution],
    policy: EnterpriseEnvironmentPolicy,
    collisions: list[CrossPipelineCollisionFinding],
) -> tuple[FleetSummaryMetrics, bool]:
    """Aggregate fleet KPIs and evaluate overall fleet policy gate status.

    Scoring formula:
    - Raw average: mean quality score across all submitted pipelines (0.0 for execution failures).
    - Blocker penalty: If any pipeline fails the policy gate or has critical blockers, the fleet score
      is strictly penalized and capped below 70.0 (max 69.9 - (blocked_policy * 5.0)) so that no
      passing score can obscure a blocked pipeline.
    """
    total = len(executions)
    successful = sum(1 for e in executions.values() if e.success)
    failed = sum(1 for e in executions.values() if not e.success)
    passed_policy = sum(1 for e in executions.values() if e.policy_passed)
    blocked_policy = sum(1 for e in executions.values() if not e.policy_passed)

    conf_dist: dict[str, int] = {}
    dec_dist: dict[str, int] = {}
    total_blockers = 0
    total_p0 = 0
    total_p1 = 0
    total_p2 = 0
    score_sum = 0.0

    for e in executions.values():
        vr = e.validation_result
        if e.success and vr:
            score_sum += getattr(vr, "quality_score", 0.0)
            conf_key = str(getattr(vr, "confidence", "INSUFFICIENT")).upper()
            conf_dist[conf_key] = conf_dist.get(conf_key, 0) + 1

            dec_key = str(getattr(vr, "final_decision", "UNKNOWN")).upper()
            dec_dist[dec_key] = dec_dist.get(dec_key, 0) + 1

            if hasattr(vr, "decision_risk_synthesis") and vr.decision_risk_synthesis:
                syn = vr.decision_risk_synthesis
                syn_blockers = getattr(syn, "blockers", None)
                if not isinstance(syn_blockers, list):
                    syn_blockers = getattr(syn, "production_blockers", None)
                if isinstance(syn_blockers, list):
                    total_blockers += len(syn_blockers)
                for r in getattr(syn, "top_risks", []):
                    if getattr(r, "severity", None) == Severity.CRITICAL:
                        total_p0 += 1
                    elif getattr(r, "severity", None) == Severity.HIGH:
                        total_p1 += 1
                    else:
                        total_p2 += 1
            elif getattr(vr, "has_blocking", False):
                total_blockers += 1
        else:
            conf_dist["INSUFFICIENT"] = conf_dist.get("INSUFFICIENT", 0) + 1
            dec_dist["EXECUTION_FAILURE"] = dec_dist.get("EXECUTION_FAILURE", 0) + 1

    # Fleet Quality Score:
    # An aggregate reporting metric representing the unweighted arithmetic mean
    # of successfully validated pipeline scores.
    # It is strictly separate from the authoritative enterprise policy gate:
    # a high fleet quality score NEVER masks an individual blocking pipeline or failure.
    # The enterprise policy gate remains authoritative over the score.
    if total == 0 or successful == 0:
        fleet_quality_score = 0.0
    else:
        fleet_quality_score = round(score_sum / successful, 2)

    # Check confirmed collisions respecting the configured tier policy
    has_critical_collision = any(c.status == CollisionStatus.CONFIRMED for c in collisions)
    collision_blocks_gate = has_critical_collision and getattr(policy, "block_on_confirmed_collisions", True)

    # Deterministic Enterprise Policy Gate:
    # Authoritative over the aggregate score. The fleet gate passes ONLY when:
    # 1. Zero pipeline executions failed or raised errors (failed == 0)
    # 2. Zero pipelines violated their tier policy (blocked_policy == 0)
    # 3. Confirmed collision does not violate tier policy (collision_blocks_gate is False)
    fleet_policy_passed = (
        failed == 0
        and blocked_policy == 0
        and not collision_blocks_gate
    )

    metrics = FleetSummaryMetrics(
        total_pipelines=total,
        successful_validations=successful,
        failed_validations=failed,
        passed_policy=passed_policy,
        blocked_policy=blocked_policy,
        fleet_quality_score=fleet_quality_score,
        confidence_distribution=conf_dist,
        decision_distribution=dec_dist,
        total_blockers=total_blockers,
        total_p0_risks=total_p0,
        total_p1_risks=total_p1,
        total_p2_risks=total_p2,
    )
    return metrics, fleet_policy_passed


class EnterpriseFleetOrchestrator:
    """Orchestrates end-to-end online validation across an enterprise fleet of pipelines."""

    def __init__(
        self,
        connector: DatabricksConnector | None = None,
        provider: DatabricksEvidenceProvider | None = None,
        max_workers: int = 4,
    ) -> None:
        """Initialize the fleet orchestrator with optional connector/provider injection."""
        self.connector = connector
        self.provider = provider
        self.max_workers = max(1, int(max_workers))

    def _execute_single_target(
        self,
        target: EnterprisePipelineTarget,
        default_workspace: str | None,
        policy: EnterpriseEnvironmentPolicy,
        resolved_token: str | None = None,
    ) -> PipelineFleetExecution:
        """Execute validation on an isolated pipeline target without affecting neighboring targets."""
        start_time = time.perf_counter()
        pid = target.id
        ws_host = target.workspace or default_workspace

        try:
            if self.connector:
                conn = self.connector
            else:
                # Use standard credential resolver for live API execution
                creds = resolve_databricks_credentials(
                    host=ws_host,
                    token=resolved_token,
                    job_id=target.job_id,
                    pipeline_id=target.pipeline_id,
                    load_env=True,
                )
                final_host = creds["host"] or ws_host
                final_token = creds["token"] or resolved_token
                if not final_host or not final_token:
                    raise ConfigurationError(
                        f"Missing Databricks host or token for pipeline '{pid}'"
                    )
                conn = LiveDatabricksConnector(host=final_host, token=final_token)

            target_env = target.environment or policy.tier.value
            orchestrator = OnlineValidationOrchestrator(connector=conn)
            validation_result = orchestrator.validate(
                job_id=target.job_id,
                pipeline_id=target.pipeline_id,
                contract_path=str(target.contract_path) if target.contract_path else None,
                code_path=str(target.code_path) if target.code_path else None,
                environment=target_env,
            )

            # Check if validation result contains primary authentication/authorization errors
            auth_error = None
            if hasattr(validation_result, "evidence_diagnostics") and validation_result.evidence_diagnostics:
                for diag in validation_result.evidence_diagnostics:
                    err_cd = getattr(diag, "error_code", None)
                    if err_cd in ("AUTHENTICATION_FAILURE", "AUTHORIZATION_FAILURE"):
                        auth_error = getattr(diag, "error_message", None) or f"Authentication failure in {diag.category}"
                        break

            if auth_error:
                return PipelineFleetExecution(
                    pipeline_id=pid,
                    target=target,
                    success=False,
                    policy_passed=False,
                    policy_violations=[f"Authentication/Authorization failure: {auth_error}"],
                    validation_result=validation_result,
                    error_message=auth_error,
                    is_auth_or_config_error=True,
                    error_provenance="AUTHENTICATION_FAILURE",
                    duration_seconds=time.perf_counter() - start_time,
                )

            # Evaluate policy
            policy_passed, violations = policy.evaluate_pipeline(validation_result)
            duration = time.perf_counter() - start_time

            return PipelineFleetExecution(
                pipeline_id=pid,
                target=target,
                success=True,
                policy_passed=policy_passed,
                policy_violations=violations,
                validation_result=validation_result,
                is_auth_or_config_error=False,
                duration_seconds=duration,
            )

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.exception("Failed validating pipeline '%s': %s", pid, exc)

            is_auth_or_config = False
            err_prov = "API_FAILURE"
            exc_str = str(exc).lower()

            if isinstance(exc, ConfigurationError):
                is_auth_or_config = True
                err_prov = "CONFIGURATION_ERROR"
            elif isinstance(exc, (DatabricksApiError, DatabricksAPIError)):
                if exc.status_code in (401, 403) or any(k in exc_str for k in ("401", "403", "auth", "token", "permission")):
                    is_auth_or_config = True
                    err_prov = "AUTHENTICATION_FAILURE"
            elif any(k in exc_str for k in ("401", "403", "auth", "token", "permission", "credentials")):
                is_auth_or_config = True
                err_prov = "AUTHENTICATION_FAILURE"

            return PipelineFleetExecution(
                pipeline_id=pid,
                target=target,
                success=False,
                policy_passed=False,
                policy_violations=[f"Pipeline execution failure: {exc}"],
                validation_result=None,
                error_message=str(exc),
                is_auth_or_config_error=is_auth_or_config,
                error_provenance=err_prov,
                duration_seconds=duration,
            )

    def validate_fleet(
        self,
        manifest: FleetManifest,
        environment_override: EnvironmentTier | None = None,
        policy: EnterpriseEnvironmentPolicy | None = None,
        policy_file: Path | None = None,
        max_workers: int | None = None,
        token: str | None = None,
    ) -> FleetValidationResult:
        """Run enterprise validation across all targets in the fleet manifest."""
        start_time = time.perf_counter()
        fleet_env = environment_override or manifest.environment

        # Resolve policy
        if policy:
            effective_policy = policy
        elif policy_file:
            try:
                raw_policy = yaml.safe_load(policy_file.read_text(encoding="utf-8")) or {}
                effective_policy = EnterpriseEnvironmentPolicy(**raw_policy)
            except Exception as exc:
                raise ConfigurationError(f"Failed loading policy file '{policy_file}': {exc}") from exc
        else:
            effective_policy = EnterpriseEnvironmentPolicy.default_for_tier(fleet_env)

        workers = max_workers or self.max_workers
        executions: dict[str, PipelineFleetExecution] = {}

        if not manifest.pipelines:
            # Handle empty fleet gracefully
            summary, policy_passed = aggregate_fleet_metrics({}, effective_policy, [])
            return FleetValidationResult(
                fleet_name=manifest.name,
                environment=fleet_env,
                policy=effective_policy,
                summary=summary,
                pipeline_executions={},
                collisions=[],
                policy_passed=policy_passed,
                duration_seconds=time.perf_counter() - start_time,
            )

        # Bounded concurrency execution
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_target = {
                executor.submit(
                    self._execute_single_target,
                    target,
                    manifest.workspace_host,
                    effective_policy,
                    token,
                ): target
                for target in manifest.pipelines
            }

            for future in concurrent.futures.as_completed(future_to_target):
                target = future_to_target[future]
                try:
                    exec_result = future.result()
                    executions[target.id] = exec_result
                except Exception as exc:
                    executions[target.id] = PipelineFleetExecution(
                        pipeline_id=target.id,
                        target=target,
                        success=False,
                        policy_passed=False,
                        policy_violations=[f"Unhandled worker error: {exc}"],
                        error_message=str(exc),
                    )

        # Cross-pipeline collision detection
        collisions = detect_fleet_collisions(executions)

        # Aggregate metrics
        summary, policy_passed = aggregate_fleet_metrics(executions, effective_policy, collisions)
        total_duration = time.perf_counter() - start_time
        has_auth_or_config = any(p.is_auth_or_config_error for p in executions.values())

        return FleetValidationResult(
            fleet_name=manifest.name,
            environment=fleet_env,
            policy=effective_policy,
            summary=summary,
            pipeline_executions=executions,
            collisions=collisions,
            policy_passed=policy_passed,
            has_auth_or_config_error=has_auth_or_config,
            duration_seconds=total_duration,
        )
