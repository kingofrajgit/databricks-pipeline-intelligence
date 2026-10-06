#!/usr/bin/env python3
"""CLI entry point for the Databricks Pipeline Intelligence Framework.

Exit-code contract (documented):
- ``0`` = validation completed successfully (even when findings contain
  FAIL — "pipeline failed" is a result, not a crash).
- non-zero = execution/system error (bad args, missing file, crash).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click
import yaml

from dpif.checkpoints.definitions import build_all_checkpoints
from dpif.checkpoints.engine import CheckpointEngine
from dpif.config import get_settings
from dpif.contract.loader import contract_cluster_job, load_contract_file
from dpif.error_handling import ConfigurationError, setup_logging
from dpif.models import (
    AnalysisMethod,
    Checkpoint,
    CheckpointStatus,
    CollectionMethod,
    DataProfile,
    SchemaColumn,
)
from dpif.rules.engine import load_rules
from dpif.scoring.engine import readiness_label, score_checkpoints

logger = setup_logging()


@click.group()
@click.version_option(version="0.1.0")
def cli() -> None:
    """Databricks Pipeline Intelligence Framework (DPIF)."""


def _repo_root(start: Path) -> Path:
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return start


def _load_metadata_profile(expected_gb: float, override: str | None = None) -> DataProfile:
    if override:
        p = Path(override)
        if not p.exists():
            raise ConfigurationError(f"Metadata profile not found: {override}")
        raw: dict[str, Any] = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        if p.suffix == ".json":
            import json

            raw = json.loads(p.read_text(encoding="utf-8"))
    else:
        here = Path(__file__).resolve()
        root = _repo_root(here)
        candidates = [
            root / "tests" / "fixtures" / "metadata",
            Path.cwd() / "tests" / "fixtures" / "metadata",
        ]
        base = next((c for c in candidates if c.is_dir()), candidates[0])
        if expected_gb <= 25:
            name = "profile_10gb.json"
        elif expected_gb <= 150:
            name = "profile_100gb.json"
        elif expected_gb <= 700:
            name = "profile_500gb.json"
        elif expected_gb <= 1500:
            name = "profile_1tb.json"
        else:
            name = "profile_3tb.json"
        p = base / name
        import json

        raw = json.loads(p.read_text(encoding="utf-8"))
    columns = [
        SchemaColumn(
            name=str(c.get("name", "")),
            data_type=str(c.get("data_type", c.get("type", "unknown"))),
            nullable=bool(c.get("nullable", True)),
        )
        for c in (raw.get("schema_columns") or [])
        if isinstance(c, dict) and c.get("name")
    ]
    return DataProfile(
        total_bytes=int(raw.get("total_bytes", 0)),
        total_gb=float(raw.get("total_gb", 0.0)),
        file_count=int(raw.get("file_count", 0)),
        average_file_size_kb=float(raw.get("average_file_size_kb", 0.0)),
        median_file_size_kb=float(raw.get("median_file_size_kb", 0.0)),
        p95_file_size_kb=float(raw.get("p95_file_size_kb", 0.0)),
        p99_file_size_kb=float(raw.get("p99_file_size_kb", 0.0)),
        min_file_size_kb=float(raw.get("min_file_size_kb", 0.0)),
        max_file_size_kb=float(raw.get("max_file_size_kb", 0.0)),
        record_count=int(raw.get("record_count", 0)),
        partition_count=int(raw.get("partition_count", 0)),
        partition_distribution=dict(raw.get("partition_distribution", {}) or {}),
        schema="",
        column_count=int(raw.get("column_count", 0)),
        null_distribution={},
        duplicate_indicators=[],
        analysis_method=AnalysisMethod.METADATA,
        # Fixture profiles are fixture evidence — never runtime scans.
        collection_method=CollectionMethod.FIXTURE,
        evidence_source="fixture metadata",
        compression=str(raw.get("compression", "")),
        partition_sizes_gb={
            str(k): float(v) for k, v in (raw.get("partition_sizes_gb", {}) or {}).items()
        },
        partition_record_counts={
            str(k): int(v) for k, v in (raw.get("partition_record_counts", {}) or {}).items()
        },
        schema_columns=columns,
    )


def _load_code_text(
    contract_path: str | None, contract_data: dict[str, Any], explicit: str | None
) -> tuple[str, str]:
    """Return (source text, display filename) for the pipeline code."""
    raw_path = contract_data.get("code_path") if isinstance(contract_data, dict) else None
    for candidate in (explicit, raw_path):
        if not candidate:
            continue
        p = Path(candidate)
        if not p.is_absolute() and contract_path:
            p = Path(contract_path).parent / p
        if p.exists():
            return p.read_text(encoding="utf-8"), _display_path(p)
        # try repo-root relative and the standard fixtures location
        root = _repo_root(Path.cwd())
        for alt in (root / str(candidate), root / "tests" / str(candidate)):
            if alt.exists():
                return alt.read_text(encoding="utf-8"), _display_path(alt)
    return "", "<code>"


def _display_path(p: Path) -> str:
    try:
        return str(p.relative_to(_repo_root(Path.cwd())))
    except ValueError:
        return p.name


@cli.command("validate")
@click.option("--contract", "-c", type=click.Path(exists=True), help="Pipeline contract YAML")
@click.option(
    "--input",
    "-i",
    "input_manifest",
    type=click.Path(exists=True),
    help="CSV manifest for multi-pipeline batch validation",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default=".",
    help="Output directory for batch report JSON/CSV",
)
@click.option("--offline/--online", default=True, help="Offline (fixtures) or live mode")
@click.option("--workspace", "--host", type=str, default=None, help="Databricks workspace URL")
@click.option("--token", type=str, default=None, help="Databricks API token")
@click.option("--job-id", type=int, default=None, help="Databricks Job ID for live mode")
@click.option("--pipeline-id", type=str, default=None, help="Databricks Pipeline (DLT) ID for live mode")
@click.option("--run-id", type=str, default=None, help="Databricks Run ID for live mode")
@click.option("--environment", "-e", type=str, default=None, help="Environment override")
@click.option("--code-path", "--code", type=click.Path(exists=True), default=None, help="Code file")
@click.option(
    "--metadata-profile",
    "--data-profile",
    type=click.Path(exists=True),
    default=None,
    help="Profile override",
)
@click.option(
    "--runtime-run",
    type=click.Path(exists=True),
    default=None,
    help="Runtime run JSON/YAML fixture",
)
@click.option(
    "--historical-runs",
    type=click.Path(exists=True),
    default=None,
    help="Path to JSON file with historical execution runs",
)
@click.option(
    "--query-history",
    type=click.Path(exists=True),
    default=None,
    help="Query-history JSON fixture for SQL operation correlation",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON output",
)
def validate_contract(
    contract: str | None,
    input_manifest: str | None,
    output_dir: str,
    offline: bool,
    job_id: int | None,
    environment: str | None,
    code_path: str | None,
    metadata_profile: str | None,
    runtime_run: str | None = None,
    historical_runs: str | None = None,
    query_history: str | None = None,
    json_output: bool = False,
    workspace: str | None = None,
    token: str | None = None,
    pipeline_id: str | None = None,
    run_id: str | None = None,
) -> None:
    """Validate a pipeline contract, multi-pipeline CSV manifest, or Databricks job.

    Examples:
      dpif validate --contract examples/customer_daily.yaml --offline
      dpif validate --input manifests/pipelines.csv --offline
      dpif validate --online --job-id 12345
    """
    settings = get_settings()
    if settings.debug:
        setup_logging(level="DEBUG")
    try:
        if contract and input_manifest:
            click.echo(
                "Error: Cannot specify both --contract and --input. "
                "Choose single-pipeline or batch validation mode.",
                err=True,
            )
            sys.exit(2)

        if offline:
            if input_manifest:
                _run_batch_offline_validation(
                    input_manifest, environment, output_dir, json_output
                )
            elif contract:
                effective_output_dir = output_dir if (output_dir and output_dir != ".") else "reports"
                _run_offline_validation(
                    contract,
                    environment,
                    code_path,
                    metadata_profile,
                    runtime_run,
                    historical_runs,
                    json_output,
                    output_dir=effective_output_dir,
                    query_history_path=query_history,
                )
            else:
                msg = "Error: Either --contract or --input is required for offline validation"
                click.echo(msg, err=True)
                sys.exit(2)
        else:
            from dpif.config import resolve_databricks_credentials

            creds = resolve_databricks_credentials(
                host=workspace,
                token=token,
                job_id=job_id,
                pipeline_id=pipeline_id,
                run_id=run_id,
                load_env=True,
            )
            resolved_workspace = creds["host"]
            resolved_token = creds["token"]
            resolved_job_id = creds["job_id"]
            resolved_pipeline_id = creds["pipeline_id"]
            resolved_run_id = creds["run_id"]

            if resolved_job_id is None and resolved_pipeline_id is None:
                click.echo(
                    "Error: Either --job-id or --pipeline-id is required for live validation",
                    err=True,
                )
                sys.exit(2)
            if not resolved_workspace:
                click.echo(
                    "Error: Databricks workspace URL is required. Provide via --workspace, DATABRICKS_HOST in environment, or .env file",
                    err=True,
                )
                sys.exit(2)
            if not resolved_token:
                click.echo(
                    "Error: Databricks token is required. Provide via --token, DATABRICKS_TOKEN in environment, or .env file",
                    err=True,
                )
                sys.exit(2)

            effective_output_dir = output_dir if (output_dir and output_dir != ".") else "reports"
            _run_live_validation(
                workspace=resolved_workspace,
                token=resolved_token,
                job_id=resolved_job_id,
                pipeline_id=resolved_pipeline_id,
                run_id=resolved_run_id,
                contract_path=contract,
                code_path=code_path,
                environment=environment,
                json_output=json_output,
                output_dir=effective_output_dir,
            )
    except SystemExit:
        raise
    except ConfigurationError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(2)
    except Exception as e:  # crash -> non-zero
        logger.exception("Validation crashed: %s", e)
        click.echo(f"Validation crashed: {e}", err=True)
        sys.exit(1)


@cli.command("validate-online")
@click.option("--workspace", "--host", type=str, default=None, help="Databricks workspace URL")
@click.option("--token", type=str, default=None, help="Databricks personal access token")
@click.option("--job-id", type=int, default=None, help="Databricks Job ID")
@click.option("--pipeline-id", type=str, default=None, help="Databricks Pipeline (DLT) ID")
@click.option("--run-id", type=str, default=None, help="Databricks Run ID")
@click.option("--contract", "-c", type=click.Path(exists=True), default=None, help="Pipeline contract YAML")
@click.option("--code-path", "--code", type=click.Path(exists=True), default=None, help="Code file")
@click.option("--environment", "-e", type=str, default=None, help="Environment override")
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default="reports",
    help="Output directory for validation reports (default: reports/)",
)
@click.option("--json", "json_output", is_flag=True, default=False, help="Emit machine-readable JSON output")
def validate_online_cmd(
    workspace: str | None,
    token: str | None,
    job_id: int | None,
    pipeline_id: str | None,
    run_id: str | None,
    contract: str | None,
    code_path: str | None,
    environment: str | None,
    output_dir: str = "reports",
    json_output: bool = False,
) -> None:
    """Validate a live Databricks job or pipeline end-to-end against the intelligence stack.

    Examples:
      dpif validate-online --job-id 12345 --workspace https://adb-123.databricks.com
      dpif validate-online --job-id 12345
      dpif validate-online --pipeline-id p-abc --contract contract.yaml
    """
    settings = get_settings()
    if settings.debug:
        setup_logging(level="DEBUG")

    from dpif.config import resolve_databricks_credentials

    creds = resolve_databricks_credentials(
        host=workspace,
        token=token,
        job_id=job_id,
        pipeline_id=pipeline_id,
        run_id=run_id,
        load_env=True,
    )
    resolved_workspace = creds["host"]
    resolved_token = creds["token"]
    resolved_job_id = creds["job_id"]
    resolved_pipeline_id = creds["pipeline_id"]
    resolved_run_id = creds["run_id"]

    if resolved_job_id is None and resolved_pipeline_id is None:
        click.echo(
            "Error: Either --job-id or --pipeline-id is required for online validation",
            err=True,
        )
        sys.exit(2)

    if not resolved_workspace:
        click.echo(
            "Error: Databricks workspace URL is required. Provide via --workspace, DATABRICKS_HOST in environment, or .env file",
            err=True,
        )
        sys.exit(2)

    if not resolved_token:
        click.echo(
            "Error: Databricks token is required. Provide via --token, DATABRICKS_TOKEN in environment, or .env file",
            err=True,
        )
        sys.exit(2)

    _run_live_validation(
        workspace=resolved_workspace,
        token=resolved_token,
        job_id=resolved_job_id,
        pipeline_id=resolved_pipeline_id,
        run_id=resolved_run_id,
        contract_path=contract,
        code_path=code_path,
        environment=environment,
        json_output=json_output,
        output_dir=output_dir,
    )


@cli.command("validate-online-fleet")
@click.option(
    "--manifest",
    "-m",
    type=click.Path(exists=True),
    required=True,
    help="Path to YAML fleet manifest file",
)
@click.option(
    "--environment",
    "-e",
    type=click.Choice(["development", "staging", "production"], case_sensitive=False),
    default=None,
    help="Target environment policy tier override",
)
@click.option(
    "--policy-file",
    type=click.Path(exists=True),
    default=None,
    help="Path to custom enterprise environment policy YAML file",
)
@click.option(
    "--export-junit",
    type=click.Path(),
    default=None,
    help="Export JUnit XML report to file",
)
@click.option(
    "--export-sarif",
    type=click.Path(),
    default=None,
    help="Export OASIS SARIF v2.1.0 report to file",
)
@click.option(
    "--export-markdown",
    type=click.Path(),
    default=None,
    help="Export Markdown PR summary to file",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON output",
)
@click.option(
    "--max-workers",
    "-w",
    type=int,
    default=4,
    help="Maximum worker threads for bounded concurrency (default: 4)",
)
def validate_online_fleet_cmd(
    manifest: str,
    environment: str | None,
    policy_file: str | None,
    export_junit: str | None,
    export_sarif: str | None,
    export_markdown: str | None,
    json_output: bool = False,
    max_workers: int = 4,
) -> None:
    """Validate an enterprise fleet of Databricks pipelines against environment policies.

    Exit codes:
      0: Fleet satisfies configured policy.
      1: Fleet validation completed but policy gate failed.
      2: System/configuration/authentication error.
    """
    settings = get_settings()
    if settings.debug:
        setup_logging(level="DEBUG")

    import dpif.reporting.enterprise_exporters as exporters
    from dpif.models.fleet import EnvironmentTier
    from dpif.orchestration.fleet import (
        EnterpriseFleetOrchestrator,
        load_and_validate_fleet_manifest,
    )

    try:
        manifest_obj = load_and_validate_fleet_manifest(manifest)
    except FileNotFoundError as e:
        click.echo(f"Error: Manifest file not found: {e}", err=True)
        sys.exit(2)
    except ConfigurationError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(2)
    except ValueError as e:
        click.echo(f"Manifest validation error: {e}", err=True)
        sys.exit(2)
    except Exception as e:
        logger.exception("Unexpected error loading manifest: %s", e)
        click.echo(f"Manifest error: {e}", err=True)
        sys.exit(2)

    env_tier: EnvironmentTier | None = None
    if environment:
        try:
            env_tier = EnvironmentTier(environment.lower())
        except ValueError:
            click.echo(f"Invalid environment tier: '{environment}'", err=True)
            sys.exit(2)

    policy_path = Path(policy_file) if policy_file else None

    try:
        orchestrator = EnterpriseFleetOrchestrator(max_workers=max_workers)
        fleet_result = orchestrator.validate_fleet(
            manifest=manifest_obj,
            environment_override=env_tier,
            policy_file=policy_path,
            max_workers=max_workers,
        )
    except ConfigurationError as e:
        click.echo(f"Configuration error during fleet validation: {e}", err=True)
        sys.exit(2)
    except Exception as e:
        logger.exception("Fleet validation crashed: %s", e)
        click.echo(f"Fleet validation crashed: {e}", err=True)
        sys.exit(2)

    # Exporters
    if export_junit:
        exporters.export_junit_xml(fleet_result, Path(export_junit))
    if export_sarif:
        exporters.export_sarif(fleet_result, Path(export_sarif))
    if export_markdown:
        exporters.export_markdown_summary(fleet_result, Path(export_markdown))

    if json_output:
        click.echo(json.dumps(fleet_result.to_dict(), indent=2))
    else:
        click.echo("=" * 60)
        click.echo("DPIF ENTERPRISE FLEET VALIDATION")
        click.echo("=" * 60)
        click.echo(f"Fleet Name:        {fleet_result.fleet_name}")
        click.echo(f"Environment:       {fleet_result.environment.value.upper()}")
        click.echo(f"Total Pipelines:   {fleet_result.summary.total_pipelines}")
        click.echo(f"Validated:         {fleet_result.summary.successful_validations}")
        click.echo(f"Execution Errors:  {fleet_result.summary.failed_validations}")
        click.echo(f"Passed Policy:     {fleet_result.summary.passed_policy}")
        click.echo(f"Blocked by Policy: {fleet_result.summary.blocked_policy}")
        click.echo(f"Fleet Score:       {fleet_result.summary.fleet_quality_score:.1f} / 100")
        click.echo(f"Total Blockers:    {fleet_result.summary.total_blockers}")
        click.echo(f"Collisions:        {len(fleet_result.collisions)}")
        click.echo("-" * 60)
        if fleet_result.policy_passed:
            click.echo("POLICY GATE:       PASSED")
        else:
            click.echo("POLICY GATE:       BLOCKED")
        click.echo("=" * 60)

    # Exit code contract:
    # 0: Fleet validation completed and policy passed
    # 1: Fleet validation completed but policy failed
    # 2: System/configuration/authentication error prevented valid evaluation
    if fleet_result.has_auth_or_config_error:
        click.echo(
            "Error: System, configuration, or authentication error prevented valid fleet evaluation.",
            err=True,
        )
        sys.exit(2)
    elif fleet_result.policy_passed:
        sys.exit(0)
    else:
        sys.exit(1)


def _run_batch_offline_validation(
    manifest_path: str,
    environment: str | None,
    output_dir: str,
    json_output: bool,
) -> None:
    from dpif.orchestration.batch import export_batch_reports, run_batch_validation
    from dpif.orchestration.manifest import load_and_validate_manifest

    valid_subs, invalid_results = load_and_validate_manifest(manifest_path)
    batch_result = run_batch_validation(valid_subs, invalid_results, environment)
    json_path, csv_path = export_batch_reports(batch_result, output_dir)

    d = batch_result.to_dict()
    sum_info = d["summary"]

    if json_output:
        click.echo(json.dumps(d, indent=2))
    else:
        click.echo("DPIF Batch Validation Complete\n")
        click.echo(f"Total pipelines: {sum_info['total_submissions']}")
        click.echo(f"Validated: {sum_info['validated']}")
        click.echo(f"Invalid submissions: {sum_info['invalid_submissions']}")
        click.echo(f"Missing inputs: {sum_info['missing_input']}")
        click.echo(f"Processing errors: {sum_info['processing_errors']}\n")
        rd = sum_info["readiness"]
        click.echo(f"Production ready: {rd['production_ready']}")
        click.echo(f"Ready with warnings: {rd['production_ready_with_warnings']}")
        click.echo(f"Not production ready: {rd['not_production_ready']}")
        click.echo(f"Insufficient evidence: {rd['insufficient_evidence']}\n")
        click.echo(f"Reports:\n  JSON: {json_path}\n  CSV:  {csv_path}")

    # Exit code contract: 0 = batch validation run completed successfully.
    # Findings, readiness states, and per-pipeline processing errors are reported in the output,
    # not treated as a CLI system crash/error.



def _run_offline_validation(
    contract_path: str,
    environment: str | None,
    code_path: str | None,
    metadata_profile: str | None,
    runtime_run_path: str | None = None,
    historical_runs_path: str | None = None,
    json_output: bool = False,
    output_dir: str = "reports",
    query_history_path: str | None = None,
) -> None:
    contract = load_contract_file(contract_path)
    if environment:
        contract.environment = environment
    with open(contract_path, encoding="utf-8") as f:
        raw_contract = yaml.safe_load(f) or {}

    profile = _load_metadata_profile(contract.expected_daily_volume_gb, metadata_profile)
    contract.source.data_profile = profile
    code_text, code_filename = _load_code_text(contract_path, raw_contract, code_path)
    cluster_config, job_config = contract_cluster_job(contract)

    # Validate rules load (surfaces YAML errors early; result unused directly
    # because the checkpoint engine loads per-category rules itself).
    load_rules()

    from dpif.code.models import AnalysisContext
    from dpif.code.parser import analyze_source

    src = contract.source
    analysis = analyze_source(code_text, filename=code_filename)
    analysis_ctx = AnalysisContext(
        pipeline_name=contract.pipeline_name,
        source_type=src.type.value,
        source_format=(src.format.value if hasattr(src.format, "value") else str(src.format)),
        expected_volume_gb=src.expected_volume_gb,
        peak_volume_gb=src.peak_volume_gb,
        processing_type=contract.processing,
        contract_name=contract.contract_id,
        evidence_source="fixture metadata",
        collection_method=profile.collection_method.value,
        code=analysis,
    )
    rule_context: dict[str, Any] = dict(analysis_ctx.to_rule_context())
    rule_context.update(
        {
            "ingestion_mode": (
                src.ingestion_mode.value
                if hasattr(src.ingestion_mode, "value")
                else str(src.ingestion_mode)
            ),
            "expected_schema": src.schema_definition.to_dict() if src.schema_definition else None,
            "jdbc": src.jdbc.to_dict() if src.jdbc else None,
            "streaming": src.streaming.to_dict() if src.streaming else None,
            "partitioning": list(src.partitioning),
        }
    )
    runtime_obj = None
    if runtime_run_path:
        from dpif.runtime.models import RuntimeRun
        from dpif.runtime.normalization import normalize_runtime_payload

        with open(runtime_run_path, encoding="utf-8") as rf:
            raw_rt = (
                json.loads(rf.read())
                if runtime_run_path.endswith(".json")
                else yaml.safe_load(rf.read())
            )
        norm_rt = normalize_runtime_payload(raw_rt)
        runtime_obj = norm_rt if isinstance(norm_rt, RuntimeRun) else RuntimeRun(**norm_rt)

    historical_objs: list[Any] | None = None
    if historical_runs_path:
        with open(historical_runs_path, encoding="utf-8") as hf:
            raw_h = (
                json.loads(hf.read())
                if historical_runs_path.endswith(".json")
                else yaml.safe_load(hf.read())
            )
        # Phase 6 (B1): preserve None-vs-[] like online. A missing file
        # means unavailable (None); a valid empty list stays [].
        if isinstance(raw_h, list):
            historical_objs = raw_h

    from dpif.flow import EvidenceState, FlowProvenance, build_pipeline_flow_graph
    from dpif.flow.correlation import normalize_query_history
    from dpif.models.implementation import EvidenceProvenanceKind

    query_entries = None
    if query_history_path:
        with open(query_history_path, encoding="utf-8") as qf:
            raw_qh = (
                json.loads(qf.read())
                if query_history_path.endswith(".json")
                else yaml.safe_load(qf.read())
            )
        # None (unavailable) vs [] (valid empty) preserved by normalize.
        query_entries = normalize_query_history(raw_qh)
    history_prov = FlowProvenance(
        kind=EvidenceProvenanceKind.FIXTURE,
        state=EvidenceState.DERIVED,
    )
    if query_entries is None and query_history_path:
        # Fixture present but unusable: keep provenance honest (UNKNOWN).
        history_prov = FlowProvenance(
            kind=EvidenceProvenanceKind.FIXTURE,
            state=EvidenceState.UNKNOWN,
        )

    from dpif.code.parser import tag_operations_with_tasks

    # Phase 9 (P9-2): attribution-only tagging for the flow graph. The single
    # offline fixture file is an exact source-file boundary, so with code
    # present every operation belongs to the single ANALYZED topology unit;
    # with no code, nothing is tagged. The combined `analysis` stays the sole
    # authority for checkpoints/rules/M5E/M5G and all decision inputs.
    flow_analysis = analysis
    task_analyses: dict[str, Any] = {}
    if code_text and code_text.strip() and analysis is not None:
        _offline_key = str(code_filename or "code")
        task_analyses = {_offline_key: analysis}
        attributed = analysis.model_copy(deep=True)
        if tag_operations_with_tasks(attributed, [(_offline_key, 1)]) > 0:
            flow_analysis = attributed
    flow_graph = build_pipeline_flow_graph(
        code_analysis=flow_analysis,
        contract=contract,
        pipeline_name=contract.pipeline_name,
        raw_code=code_text,
        data_profile=profile,
        runtime_run=runtime_obj,
        query_history=query_entries,
        history_provenance=history_prov if query_history_path else None,
    )
    # Phase 4: M5E consumes rule_context, so the code text and flow graph
    # must be visible there (minimal wiring for completeness evidence).
    rule_context["code_snippet"] = code_text
    rule_context["flow_graph"] = flow_graph

    context: dict[str, Any] = {
        "pipeline_name": contract.pipeline_name,
        "pipeline_contract": contract,
        "source": src,
        "data_profile": profile,
        "code_snippet": code_text,
        "code_filename": code_filename,
        "code_analysis": analysis,
        "flow_graph": flow_graph,
        "cluster_config": cluster_config,
        "job_config": job_config,
        "assumptions": {"mode": "offline-fixture"},
        "rule_context": rule_context,
        "runtime_run": runtime_obj,
        "runtime_data": runtime_obj,
        "historical_runs": historical_objs,
        # Phase 8: offline task-coverage parity with online. Fixture code is
        # a single analyzed unit; absent code leaves no topology so the
        # existing code-UNKNOWN paths behave exactly as before.
        "task_topology": (
            [
                {
                    "task_key": code_filename or "code",
                    "task_type": "sql" if str(code_filename).endswith(".sql") else "python",
                    "depends_on": [],
                    "resource": code_filename or "",
                    "coverage_state": "ANALYZED",
                    "detail": "offline fixture code analyzed",
                    "code_refs": [code_filename or ""],
                }
            ]
            if code_text and code_text.strip()
            else []
        ),
        # Phase 9 (P9-2): attribution-only per-task analyses. No consumer
        # reads this key; it exists for attribution metadata only.
        "task_analyses": task_analyses,
    }

    engine = CheckpointEngine()
    checkpoints = build_all_checkpoints(
        contract=contract,
        data_profile=profile,
        code_text=code_text,
        cluster_config=cluster_config,
        job_config=job_config,
        small_file_threshold_kb=get_settings().small_file_threshold_kb,
        runtime_data=runtime_obj,
        historical_runs=historical_objs,
    )
    results = engine.run_all_checkpoints(checkpoints, context)
    score, has_blocking = score_checkpoints(results)
    has_fail = any(cp.status == CheckpointStatus.FAIL for cp in results.values())
    has_unknown_or_warn = any(
        cp.status in (CheckpointStatus.UNKNOWN, CheckpointStatus.WARN) for cp in results.values()
    )
    readiness = readiness_label(score, has_blocking, has_fail, has_unknown_or_warn)

    from dpif.analyzers.data.coverage import compute_coverage
    from dpif.analyzers.data.growth import project_growth

    coverage = compute_coverage(results)
    growth = project_growth(
        contract.source.expected_volume_gb,
        contract.source.growth_rate_percent,
        contract.scalability.forecast_horizon_days,
    )

    from dpif.code.flow import build_flow

    correlations = []
    if runtime_obj:
        from dpif.runtime.correlation import correlate_static_and_runtime

        code_cp = results.get("CP-004")
        perf_cp = results.get("CP-008")
        code_findings = list(code_cp.findings) if code_cp else []
        perf_findings = list(perf_cp.findings) if perf_cp else []
        correlations = correlate_static_and_runtime(
            code_findings, perf_findings, has_runtime_evidence=bool(runtime_obj)
        )

    from dpif.analyzers.implementation import DeveloperImplementationAnalyzer

    impl_analyzer = DeveloperImplementationAnalyzer(analysis, context=rule_context)
    impl_assessment = impl_analyzer.analyze()

    context["implementation_forensics"] = impl_assessment

    from dpif.analyzers.rerun import RerunIdempotencyAnalyzer

    rerun_ctx = dict(rule_context)
    rerun_ctx.update(context)
    rerun_analyzer = RerunIdempotencyAnalyzer(analysis, context=rerun_ctx)
    rerun_assessment = rerun_analyzer.analyze()

    context["rerun_analysis"] = rerun_assessment

    from dpif.analyzers.alignment import ThreeLayerAlignmentAnalyzer

    alignment_ctx = dict(rule_context)
    alignment_ctx.update(context)
    alignment_analyzer = ThreeLayerAlignmentAnalyzer(analysis, context=alignment_ctx)
    alignment_assessment = alignment_analyzer.analyze()

    context["alignment_analysis"] = alignment_assessment

    from dpif.analyzers.sufficiency import EvidenceSufficiencyAnalyzer

    sufficiency_ctx = dict(rule_context)
    sufficiency_ctx.update(context)
    sufficiency_ctx["checkpoints"] = results
    sufficiency_analyzer = EvidenceSufficiencyAnalyzer(analysis, context=sufficiency_ctx)
    sufficiency_assessment = sufficiency_analyzer.analyze()

    context["evidence_sufficiency"] = sufficiency_assessment

    assessment = context.get("production_readiness_assessment")
    if assessment is None:
        from dpif.readiness.engine import evaluate_production_readiness

        assessment = evaluate_production_readiness(
            checkpoints=results,
            contract=contract,
            profile=profile,
            cluster_config=cluster_config,
            job_config=job_config,
            runtime_data=runtime_obj,
            historical_runs=historical_objs,
        )

    from dpif.analyzers.synthesis import DecisionRiskSynthesisAnalyzer

    synthesis_analyzer = DecisionRiskSynthesisAnalyzer(
        checkpoints=results,
        readiness=assessment,
        implementation_forensics=impl_assessment,
        rerun_analysis=rerun_assessment,
        alignment_analysis=alignment_assessment,
        evidence_sufficiency=sufficiency_assessment,
        contract=contract,
        profile=profile,
        context=context,
    )
    synthesis_assessment = synthesis_analyzer.analyze()
    context["decision_risk_synthesis"] = synthesis_assessment

    from dpif.reporting.validation_reports import persist_offline_validation_report

    json_path, md_path = persist_offline_validation_report(
        contract=contract,
        checkpoints=results,
        score=score,
        readiness_label=readiness,
        impl_assessment=impl_assessment,
        rerun_assessment=rerun_assessment,
        alignment_assessment=alignment_assessment,
        sufficiency_assessment=sufficiency_assessment,
        assessment=assessment,
        synthesis_assessment=synthesis_assessment,
        output_dir=output_dir,
        flow_graph=flow_graph,
    )

    if json_output:
        payload = assessment.to_dict()
        payload["implementation_forensics"] = impl_assessment.to_dict()
        payload["rerun_analysis"] = rerun_assessment.to_dict()
        payload["alignment_analysis"] = alignment_assessment.to_dict()
        payload["evidence_sufficiency"] = sufficiency_assessment.to_dict()
        payload["decision_risk_synthesis"] = synthesis_assessment.to_dict()
        payload["pipeline_flow_graph"] = flow_graph.to_dict()
        payload["report_paths"] = {"json": str(json_path), "markdown": str(md_path)}
        payload["checkpoints"] = {
            k: {
                "checkpoint_id": v.checkpoint_id,
                "name": v.name,
                "status": v.status.value,
                "severity": v.severity.value,
                "score": v.score,
                "findings_count": len(v.findings),
            }
            for k, v in sorted(results.items())
        }
        click.echo(json.dumps(payload, indent=2))
        return

    _display_header(contract, offline=True)
    _display_checkpoint_results(results)
    _display_data_section(contract, profile, coverage, growth, results)
    _display_code_section(analysis, build_flow(analysis), results, profile.total_gb)
    _display_implementation_forensics_section(impl_assessment)
    _display_rerun_analysis_section(rerun_assessment)
    _display_alignment_forensics_section(alignment_assessment)
    _display_evidence_sufficiency_section(sufficiency_assessment)
    if runtime_obj:
        _display_performance_section(runtime_obj, results)
        _display_correlation_section(correlations)
    _display_scalability_section(results, contract, profile, runtime_obj, historical_objs)
    _display_findings(results)
    _display_score(score, readiness)
    _display_final_readiness_section(assessment)
    _display_decision_risk_synthesis_section(synthesis_assessment)

    click.echo("")
    click.echo("=" * 60)
    click.echo("VALIDATION REPORTS PERSISTED")
    click.echo("=" * 60)
    click.echo(f"JSON report:\n  {json_path}\n")
    click.echo(f"Markdown report:\n  {md_path}\n")
    # Exit 0: validation completed (even with FAIL findings).


def _run_live_validation(
    workspace: str | None,
    token: str | None,
    job_id: int | str | None,
    pipeline_id: str | None,
    run_id: int | str | None,
    contract_path: str | None,
    code_path: str | None,
    environment: str | None,
    json_output: bool = False,
    output_dir: str = "reports",
) -> None:
    from dpif.connectors.live import DatabricksApiError
    from dpif.orchestration.online import run_online_validation
    from dpif.providers.base import mask_sensitive_credentials
    from dpif.reporting.validation_reports import persist_online_validation_report

    try:
        result = run_online_validation(
            workspace=workspace,
            token=token,
            job_id=job_id,
            pipeline_id=pipeline_id,
            run_id=run_id,
            contract_path=contract_path,
            code_path=code_path,
            environment=environment,
        )
    except DatabricksApiError as e:
        clean_msg = mask_sensitive_credentials(str(e), token)
        click.echo(f"Databricks API Error: {clean_msg}", err=True)
        sys.exit(2)
    except ValueError as e:
        clean_msg = mask_sensitive_credentials(str(e), token)
        click.echo(f"Validation Error: {clean_msg}", err=True)
        sys.exit(2)
    except Exception as e:
        clean_msg = mask_sensitive_credentials(str(e), token)
        click.echo(f"Validation crashed: {clean_msg}", err=True)
        sys.exit(1)

    json_path, md_path = persist_online_validation_report(result, output_dir=output_dir)

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2))
        return

    _display_online_validation(result)

    click.echo("=" * 60)
    click.echo("VALIDATION REPORTS PERSISTED")
    click.echo("=" * 60)
    click.echo(f"JSON report:\n  {json_path}\n")
    click.echo(f"Markdown report:\n  {md_path}\n")


def _display_online_validation(result: Any) -> None:
    click.echo("=" * 60)
    click.echo("DPIF ONLINE VALIDATION")
    click.echo("=" * 60)
    click.echo("")
    click.echo("WORKSPACE:")
    click.echo(f"    {result.workspace or 'UNKNOWN'}")
    click.echo("")
    click.echo("RESOURCE:")
    click.echo(f"    {result.resource_type.upper()} {result.resource_id}")
    click.echo("")
    click.echo("EVIDENCE:")
    order = ["workspace", "job", "pipeline", "cluster", "runtime", "historical", "code"]
    labels = {
        "workspace": "Workspace",
        "job": "Job",
        "pipeline": "Pipeline",
        "cluster": "Cluster",
        "runtime": "Runtime",
        "historical": "Historical",
        "code": "Code",
    }
    for cat in order:
        if cat in result.evidence_summary:
            st = result.evidence_summary[cat]
            lbl = labels.get(cat, cat.capitalize())
            click.echo(f"    {lbl:<16}{st}")
    click.echo("")

    _display_findings(result.checkpoints)

    if result.evidence_sufficiency:
        click.echo("-" * 60)
        click.echo("M5H EVIDENCE COVERAGE")
        click.echo("-" * 60)
        click.echo("Coverage:")
        click.echo(f"    {result.evidence_sufficiency.coverage_score:.0f}%")
        click.echo("")
        click.echo("Confidence:")
        click.echo(f"    {result.confidence}")
        click.echo("")
        suff_str = "TRUE" if result.decision_sufficiency else "FALSE"
        click.echo("Decision Sufficiency:")
        click.echo(f"    {suff_str}")
        click.echo("")
        if result.evidence_sufficiency.critical_missing_evidence:
            click.echo("Missing Evidence:")
            for m in result.evidence_sufficiency.critical_missing_evidence[:5]:
                click.echo(f"    - {m}")
            click.echo("")

    if result.decision_risk_synthesis:
        _display_decision_risk_synthesis_section(result.decision_risk_synthesis)


def _display_header(contract: Any, offline: bool) -> None:
    click.echo("=" * 60)
    click.echo("DATABRICKS PIPELINE INTELLIGENCE - Offline Validation")
    click.echo("=" * 60)
    click.echo("")
    click.echo("DPIF Validation")
    click.echo("")
    click.echo(f"    Pipeline:    {contract.pipeline_name}")
    click.echo(f"    Environment: {contract.environment}")
    mode = "OFFLINE (fixture metadata, not runtime measurements)" if offline else "LIVE"
    click.echo(f"    Mode:        {mode}")
    click.echo("")


def _display_checkpoint_results(results: dict[str, Checkpoint]) -> None:
    click.echo("CHECKPOINTS")
    click.echo("-" * 60)
    labels = {
        "CP-001": "Source",
        "CP-004": "Code",
        "CP-007": "Data",
        "CP-008": "Performance",
        "CP-009": "Cluster",
        "CP-010": "Scalability",
        "CP-011": "Job",
        "CP-012": "Incremental",
        "CP-013": "ErrorHandling",
        "CP-014": "Retry",
        "CP-015": "Restart",
        "CP-016": "Idempotency",
        "CP-019": "Cost",
        "CP-020": "Security",
        "CP-021": "Governance",
        "CP-022": "DataQuality",
        "CP-023": "SLA",
        "CP-024": "Readiness",
    }
    order = [
        "CP-001",
        "CP-007",
        "CP-004",
        "CP-008",
        "CP-009",
        "CP-010",
        "CP-011",
        "CP-012",
        "CP-013",
        "CP-014",
        "CP-015",
        "CP-016",
        "CP-019",
        "CP-020",
        "CP-021",
        "CP-022",
        "CP-023",
        "CP-024",
    ]
    for cp_id in order:
        cp = results.get(cp_id)
        if cp is None:
            continue
        label = labels.get(cp_id, cp_id)
        click.echo(f"    {label:<14}: {cp.status.value}")
    click.echo("")


def _fmt_gb(value: float | None) -> str:
    if value is None:
        return "UNKNOWN (not configured)"
    if value >= 1024:
        return f"{value / 1024:.1f} TB/day"
    return f"{value:.0f} GB/day"


def _display_data_section(
    contract: Any, profile: Any, coverage: Any, growth: dict, results: dict
) -> None:
    click.echo("SOURCE")
    src_cp = results.get("CP-001")
    click.echo(f"  {src_cp.status.value if src_cp else 'UNKNOWN'}")
    click.echo("")
    click.echo("DATA")
    data_cp = results.get("CP-007")
    click.echo(f"  {data_cp.status.value if data_cp else 'UNKNOWN'}")
    click.echo("")
    click.echo("Evidence Coverage:")
    click.echo(
        f"  {coverage.coverage_percentage:.0f}% "
        f"({coverage.evaluated_checks}/{coverage.total_checks} checks evidenced)"
    )
    click.echo("")
    click.echo("Data Volume:")
    click.echo(f"  {_fmt_gb(contract.source.expected_volume_gb)}")
    click.echo("")
    click.echo("Peak:")
    click.echo(f"  {_fmt_gb(contract.source.peak_volume_gb)}")
    click.echo("")
    click.echo("File Count:")
    click.echo(f"  {profile.file_count:,}")
    click.echo("")
    click.echo("Average File Size:")
    click.echo(f"  {profile.average_file_size_kb:,.0f} KB")
    click.echo("")
    click.echo(
        f"Evidence source: fixture metadata (collection method: {profile.collection_method.value})"
    )
    click.echo("")
    if growth.get("status") == "OK":
        horizon = growth["observed"]["horizon_days"]
        click.echo(f"Projected ({horizon}d at declared growth rate):")
        click.echo(f"  {_fmt_gb(growth['projected_gb'])}")
    else:
        click.echo("Projected:")
        click.echo("  UNKNOWN (no growth rate declared; not invented)")
    click.echo("")
    click.echo("Runtime prediction unavailable (insufficient historical execution data).")
    click.echo("")
    data_findings = [
        f for cp_id in ("CP-001", "CP-007") if (cp := results.get(cp_id)) for f in cp.findings
    ]
    click.echo("Findings:")
    if data_findings:
        for f in data_findings:
            click.echo(f"  {f.rule_id} {f.title or f.name}")
    else:
        click.echo("  (none)")
    click.echo("")


def _display_code_section(
    analysis: Any, flow: dict[str, list[str]], results: dict, volume_gb: float
) -> None:
    click.echo("CODE")
    code_cp = results.get("CP-004")
    click.echo(f"  {code_cp.status.value if code_cp else 'UNKNOWN'}")
    counts = analysis.operation_counts() if analysis is not None else {}
    total_ops = sum(counts.values())
    click.echo("")
    click.echo(f"Operations detected: {total_ops}")
    for op_name in sorted(counts):
        click.echo(f"  {op_name}: {counts[op_name]}")
    if analysis is not None and analysis.parse_error:
        click.echo(f"  Parse status: ERROR ({analysis.parse_error})")
    if flow:
        click.echo("")
        click.echo("DataFrame flow:")
        for df_name in sorted(flow)[:8]:
            click.echo(f"  {df_name}: {' -> '.join(flow[df_name][:10])}")
    findings = list(code_cp.findings) if code_cp else []
    if findings:
        click.echo("")
        click.echo("Code findings:")
        for f in findings:
            click.echo(f"  {f.rule_id} [{f.severity.value}] {f.title or f.name}")
            for ev in f.evidence.evidence[:4]:
                click.echo(f"    Evidence: {_mask_secrets(str(ev))[:200]}")
            click.echo(f"    Data context: Expected input: {volume_gb:.0f} GB/day")
            if f.recommendation:
                click.echo(f"    Recommendation: {f.recommendation[:240]}")
    click.echo("")


def _display_implementation_forensics_section(impl_assessment: Any) -> None:
    click.echo("DEVELOPER IMPLEMENTATION FORENSICS (M5E)")
    click.echo("-" * 60)
    click.echo(f"  Overall Forensics Status: {impl_assessment.overall_status.value}")
    click.echo("")
    click.echo("  Dimensions Assessment:")
    for dim_key, dim_val in sorted(impl_assessment.dimensions.items()):
        status_str = dim_val.status.value
        click.echo(f"    - {dim_key:<32}: [{status_str:<4}] {dim_val.findings_count} finding(s)")
    if impl_assessment.all_findings:
        click.echo("")
        click.echo("  Forensic Findings Detail:")
        for f in impl_assessment.all_findings:
            prov = f.provenance.value if hasattr(f.provenance, "value") else str(f.provenance)
            click.echo(f"    [{f.status.value}] {f.rule_id} - {f.title} ({f.location}) [{prov}]")
            click.echo(f"      Description: {f.description}")
            if f.recommendation:
                click.echo(f"      Recommendation: {f.recommendation}")
    click.echo("")


def _display_rerun_analysis_section(rerun_assessment: Any) -> None:
    click.echo("RERUN / IDEMPOTENCY FORENSICS (M5F)")
    click.echo("-" * 60)
    click.echo(f"  Overall Idempotency Status: {rerun_assessment.idempotency.overall_status.value}")
    click.echo(f"  Pipeline Name:              {rerun_assessment.pipeline_name}")
    click.echo("")
    click.echo("  Rerun Analysis (7 Scenarios):")
    for sc_name, sc_data in sorted(rerun_assessment.scenarios.items()):
        click.echo(f"    - {sc_name:<25}: [{sc_data.status.value:<4}] {sc_data.risk_summary}")
    click.echo("")
    click.echo("  Idempotency Dimensions:")
    for dim_name, dim_data in sorted(rerun_assessment.idempotency.dimensions.items()):
        click.echo(f"    - {dim_name:<30}: [{dim_data.status.value:<4}] {dim_data.summary}")
    click.echo("")
    click.echo("  Duplicate-Data Risk:")
    click.echo(
        f"    Status:     [{rerun_assessment.duplicate_risk.status.value}] ({rerun_assessment.duplicate_risk.risk_level.value})"
    )
    click.echo(f"    Summary:    {rerun_assessment.duplicate_risk.summary}")
    if rerun_assessment.duplicate_risk.potential_duplicate_sources:
        for src in rerun_assessment.duplicate_risk.potential_duplicate_sources:
            click.echo(f"      * {src}")
    click.echo("")
    click.echo("  Data-Loss Risk:")
    click.echo(
        f"    Status:     [{rerun_assessment.data_loss_risk.status.value}] ({rerun_assessment.data_loss_risk.risk_level.value})"
    )
    click.echo(f"    Summary:    {rerun_assessment.data_loss_risk.summary}")
    if rerun_assessment.data_loss_risk.potential_data_loss_sources:
        for src in rerun_assessment.data_loss_risk.potential_data_loss_sources:
            click.echo(f"      * {src}")
    click.echo("")
    retry_sc = rerun_assessment.scenarios.get("JOB_RETRY")
    click.echo("  Retry Safety:")
    if retry_sc:
        click.echo(f"    [{retry_sc.status.value}] {retry_sc.risk_summary}")
    click.echo("")
    conc_sc = rerun_assessment.scenarios.get("CONCURRENT_EXECUTION")
    click.echo("  Concurrency Safety:")
    if conc_sc:
        click.echo(f"    [{conc_sc.status.value}] {conc_sc.risk_summary}")
    click.echo("")
    late_sc = rerun_assessment.scenarios.get("LATE_ARRIVING_DATA")
    click.echo("  Late-Data Handling:")
    if late_sc:
        click.echo(f"    [{late_sc.status.value}] {late_sc.risk_summary}")
    click.echo("")


def _display_alignment_forensics_section(alignment_assessment: Any) -> None:
    click.echo("THREE-LAYER ALIGNMENT & DRIFT FORENSICS (M5G)")
    click.echo("-" * 60)
    click.echo(f"  Overall Alignment Status: [{alignment_assessment.overall_status.value}]")
    click.echo(f"  Drift Severity:           [{alignment_assessment.drift_severity.value}]")
    blocking_count = sum(1 for f in alignment_assessment.findings if f.blocking)
    click.echo(
        f"  Total Drifts Detected:    {alignment_assessment.total_drifts} "
        f"({blocking_count} blocking)"
    )
    click.echo("")
    click.echo("  Dimensions Assessment (9 Dimensions):")
    for dim_name, dim_data in sorted(alignment_assessment.dimensions.items()):
        click.echo(
            f"    - {dim_name:<26}: [{dim_data.status.value:<4}] ({dim_data.drift_severity.value:<8}) {dim_data.summary}"
        )
    if alignment_assessment.findings:
        click.echo("")
        click.echo("  Layer Divergence Details:")
        for f in alignment_assessment.findings:
            prov = getattr(f.provenance, "value", str(f.provenance))
            click.echo(
                f"    [{f.status.value}] {f.rule_id} - {f.title} ({f.divergence_kind.value}) [{prov}]"
            )
            click.echo(f"      Expected:       {f.expected}")
            click.echo(f"      Implemented:    {f.implemented}")
            click.echo(f"      Actual:         {f.actual}")
            click.echo(f"      Description:    {f.description}")
            if f.recommendation:
                click.echo(f"      Recommendation: {f.recommendation}")
    click.echo("")


def _display_evidence_sufficiency_section(sufficiency: Any) -> None:
    click.echo("EVIDENCE COVERAGE & DECISION SUFFICIENCY (M5H)")
    click.echo("-" * 60)
    click.echo(f"  Overall Confidence:       [{sufficiency.overall_confidence.value}]")
    suff_label = "SUFFICIENT" if sufficiency.overall_decision_sufficiency else "INSUFFICIENT"
    click.echo(f"  Decision Sufficiency:     [{suff_label}]")
    click.echo(
        f"  Evidence Coverage Score:  {sufficiency.coverage_score}% "
        f"({sufficiency.domains_sufficient}/{sufficiency.domains_evaluated} domains sufficient)"
    )
    click.echo("")
    click.echo("  16-Domain Evidence Coverage:")
    for d_name, d_cov in sorted(sufficiency.domain_coverages.items()):
        status_tag = "SUFFICIENT" if d_cov.decision_sufficient else "INSUFFICIENT"
        click.echo(
            f"    - {d_name:<26}: [{d_cov.confidence.value:<12}] [{d_cov.quality.value:<12}] "
            f"({int(d_cov.completeness_score * 100):>3}% complete) [{status_tag}]"
        )
    click.echo("")
    click.echo("  Key Pipeline Decisions:")
    for dec in sufficiency.decisions:
        dec_suff = "SUFFICIENT" if dec.is_sufficient else "INSUFFICIENT"
        click.echo(
            f"    [{dec.decision_status:<7}] {dec.decision_name:<24} [{dec.confidence.value:<12}] [{dec_suff}]"
        )
        click.echo(f"      Rationale: {dec.rationale}")
        if dec.required_evidence:
            req_str = "; ".join(dec.required_evidence)
            click.echo(f"      Required Evidence: {req_str}")
    if sufficiency.critical_missing_evidence:
        click.echo("")
        click.echo("  Critical Missing Evidence Required for Release:")
        for req in sufficiency.critical_missing_evidence[:5]:
            click.echo(f"    * {req}")
    click.echo("")


def _display_performance_section(runtime_run: Any, results: dict) -> None:
    click.echo("PERFORMANCE (RUNTIME)")
    perf_cp = results.get("CP-008")
    click.echo(f"  {perf_cp.status.value if perf_cp else 'UNKNOWN'}")
    click.echo(f"  Run ID: {runtime_run.run_id}")
    dur_s = (runtime_run.execution_duration_ms or 0) / 1000.0
    click.echo(f"  Duration: {dur_s:.1f}s")
    shuff_gb = (runtime_run.total_shuffle_bytes or 0) / (1024**3)
    click.echo(f"  Total Shuffle: {shuff_gb:.2f} GB")
    spill_gb = (runtime_run.total_spill_bytes or 0) / (1024**3)
    click.echo(f"  Total Spill: {spill_gb:.2f} GB")
    click.echo(f"  GC Time Ratio: {runtime_run.gc_ratio * 100:.1f}%")
    findings = list(perf_cp.findings) if perf_cp else []
    if findings:
        click.echo("")
        click.echo("Performance findings:")
        for f in findings:
            click.echo(f"  {f.rule_id} [{f.severity.value}] {f.title or f.name}")
            for ev in f.evidence.evidence[:4]:
                click.echo(f"    Evidence: {_mask_secrets(str(ev))[:200]}")
            if f.recommendation:
                click.echo(f"    Recommendation: {f.recommendation[:240]}")
    click.echo("")


def _display_correlation_section(correlations: list[Any]) -> None:
    if not correlations:
        return
    click.echo("STATIC <-> RUNTIME CORRELATION")
    click.echo("-" * 60)
    for c in correlations:
        r_ids = ", ".join(c.runtime_rule_ids) if c.runtime_rule_ids else "none"
        click.echo(f"  [{c.status}] {c.static_rule_id} -> {r_ids}")
        click.echo(f"    Confidence: {c.confidence:.2f}")
        click.echo(f"    Rationale: {c.description}")
    click.echo("")


def _display_scalability_section(
    results: dict[str, Checkpoint],
    contract: Any,
    profile: Any,
    runtime_obj: Any,
    historical_runs: list[Any] | None,
) -> None:
    from dpif.scalability.engine import analyze_historical_trends, generate_scenarios

    b, e, p, g = generate_scenarios(contract, profile, runtime_obj)
    scenarios = [s for s in [b, e, p, *g] if s is not None]

    click.echo("SCALABILITY INTELLIGENCE")
    click.echo("-" * 60)
    click.echo("  Workload Scenarios:")
    for s in scenarios:
        vol_str = (
            f"{s.input_volume_gb:.1f} GB"
            if s.input_volume_gb < 1024
            else f"{s.input_volume_tb:.1f} TB"
        )
        st_val = s.scenario_type.value
        ev_val = s.evidence_source.value
        click.echo(f"    [{st_val:<10}] {s.name:<34}: {vol_str:>10} ({ev_val})")

    if historical_runs and len(historical_runs) >= 2:
        trends = analyze_historical_trends(historical_runs)
        rt_trend = next((t for t in trends if t.metric_name == "runtime_scaling"), None)
        if rt_trend and rt_trend.scaling_behavior.value != "INSUFFICIENT_DATA":
            click.echo(
                f"  Observed Scaling Trend: {rt_trend.scaling_behavior.value} "
                f"(scaling index: {rt_trend.scaling_factor})"
            )

    cp = results.get("CP-010")
    if cp and cp.findings:
        click.echo("  Scalability Findings:")
        for f in cp.findings:
            click.echo(f"    [{f.status.value}] {f.rule_id} {f.name} ({f.severity.value})")
            if f.recommendation:
                click.echo(f"      Recommendation: {f.recommendation[:200]}")
    click.echo("")


def _mask_secrets(text: str) -> str:
    import re

    return re.sub(
        r"(?i)(password|passwd|secret|token|api[_-]?key)\s*=\s*(['\"])[^'\"]*(['\"])",
        r"\1 = \2***masked***\3",
        text,
    )


def _display_findings(results: dict[str, Checkpoint]) -> None:
    shown = 0
    for cp in results.values():
        for f in cp.findings:
            shown += 1
            if shown == 1:
                click.echo("FINDINGS")
                click.echo("-" * 60)
            click.echo(f"    {f.rule_id}")
            click.echo(f"    Severity: {f.severity.value}")
            click.echo(f"    Finding: {f.title or f.name}")
            if f.description:
                click.echo(f"    Description: {f.description}")
            for ev in f.evidence.evidence[:3]:
                click.echo(f"    Evidence: {_mask_secrets(str(ev))[:200]}")
            if f.recommendation:
                click.echo(f"    Recommendation: {f.recommendation[:300]}")
            click.echo(f"    Confidence: {f.confidence:.2f}")
            click.echo(f"    Blocking: {str(f.blocking).lower()}")
            click.echo("")
    unknowns = [cp for cp in results.values() if cp.status == CheckpointStatus.UNKNOWN]
    if unknowns:
        click.echo("UNKNOWN (insufficient evidence — not PASS)")
        click.echo("-" * 60)
        for cp in unknowns:
            reason = (
                cp.assumptions.get("unknown-reason")
                or cp.evidence.recommendation
                or "evidence unavailable"
            )
            click.echo(f"    {cp.checkpoint_id} {cp.name}: {reason}")
        click.echo("")


def _display_score(score: Any, readiness: str) -> None:
    click.echo("SCORE")
    click.echo("-" * 60)
    for cat, val in sorted(score.categories.items()):
        click.echo(f"    {cat:<14}: {val:5.1f}")
    click.echo("")
    click.echo(f"    Overall Score: {score.overall:.0f}/100")
    click.echo(f"    Status: {readiness}")
    click.echo("")


def _display_final_readiness_section(assessment: Any) -> None:
    click.echo("=" * 60)
    click.echo("CP-FINAL — PRODUCTION READINESS DECISION")
    click.echo("=" * 60)
    click.echo(f"    Final Status:       {assessment.status.value}")
    click.echo(f"    Quality Score:      {assessment.quality_score:.1f}/100")
    cov_pct = assessment.evidence_coverage
    click.echo(f"    Evidence Coverage:  {cov_pct:.1f}%")
    click.echo("")
    click.echo("    Domain Coverage:")
    for dom, cov in sorted(assessment.comprehensive_coverage.domain_coverages.items()):
        tier_str = getattr(cov.quality_tier, "value", str(cov.quality_tier))
        click.echo(f"      - {dom:<16}: {cov.coverage_percentage:5.1f}% [{tier_str}]")
    click.echo("")
    if assessment.cross_domain_risks:
        click.echo("    Synthesized Cross-Domain Risks:")
        for r in assessment.cross_domain_risks:
            sev = r.severity.value if hasattr(r.severity, "value") else str(r.severity)
            click.echo(f"      - [{sev}] {r.title}")
            click.echo(f"        {r.description[:90]}")
        click.echo("")
    click.echo("    Decision Reasons:")
    for r in assessment.decision_reasons:
        click.echo(f"      - {r}")
    click.echo("")
    if assessment.required_actions:
        click.echo("    Prioritized Required Actions:")
        for act in assessment.required_actions:
            pri = act.priority.value if hasattr(act.priority, "value") else str(act.priority)
            click.echo(f"      - [{pri}] {act.title}")
            if act.recommendation:
                click.echo(f"        Action: {act.recommendation[:85]}")
        click.echo("")


def _display_decision_risk_synthesis_section(synthesis: Any) -> None:
    click.echo("=" * 60)
    click.echo("DECISION & RISK SYNTHESIS (M5I)")
    click.echo("=" * 60)
    click.echo(f"    FINAL DECISION:        {synthesis.final_decision.value}")
    click.echo(f"    CONFIDENCE:            {synthesis.confidence.value}")
    suff_str = "TRUE" if synthesis.decision_sufficiency else "FALSE"
    click.echo(f"    DECISION SUFFICIENCY:  {suff_str}")
    band_str = f" ({synthesis.score_band})" if hasattr(synthesis, "score_band") and synthesis.score_band else ""
    click.echo(f"    QUALITY SCORE:         {synthesis.quality_score:.1f}/100{band_str}")
    if synthesis.score_override_reason:
        click.echo(f"    SCORE OVERRIDE REASON: {synthesis.score_override_reason}")
    click.echo("")

    if synthesis.decision_explanation:
        click.echo("    DECISION RATIONALE:")
        for idx, exp in enumerate(synthesis.decision_explanation, 1):
            click.echo(f"      {idx}. {exp}")
        click.echo("")

    if synthesis.blockers:
        click.echo(f"    BLOCKERS ({len(synthesis.blockers)}):")
        for idx, b in enumerate(synthesis.blockers, 1):
            click.echo(f"      {idx}. [{b.severity.value}] {b.title}")
            click.echo(f"         Resolution: {b.resolution_requirement}")
        click.echo("")

    if synthesis.top_risks:
        click.echo(f"    TOP RISKS ({len(synthesis.top_risks)}):")
        for idx, r in enumerate(synthesis.top_risks, 1):
            click.echo(f"      {idx}. [{r.severity.value}] {r.title} ({r.category.value})")
            if r.consequence:
                click.echo(f"         Consequence: {r.consequence[:90]}")
        click.echo("")

    if synthesis.risk_chains:
        click.echo("    CAUSAL RISK CHAINS:")
        for c in synthesis.risk_chains[:2]:
            click.echo(f"      - {c.title}:")
            click.echo(f"        {' -> '.join(c.steps[:3])} -> {c.ultimate_impact}")
        click.echo("")

    if synthesis.missing_evidence:
        click.echo("    MISSING EVIDENCE:")
        for idx, ev in enumerate(synthesis.missing_evidence[:5], 1):
            click.echo(f"      {idx}. {ev}")
        click.echo("")

    if synthesis.remediations:
        click.echo("    REQUIRED ACTIONS:")
        for idx, rem in enumerate(synthesis.remediations[:5], 1):
            click.echo(f"      {idx}. [{rem.priority}] {rem.title}")
            if rem.description:
                click.echo(f"         Action: {rem.description[:85]}")
        click.echo("")


def main() -> None:
    """Main entry point for the dpif CLI."""
    cli()


if __name__ == "__main__":
    main()
