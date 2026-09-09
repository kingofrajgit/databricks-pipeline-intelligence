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
@click.option("--offline/--online", default=True, help="Offline (fixtures) or live mode")
@click.option("--job-id", type=int, default=None, help="Databricks Job ID for live mode")
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
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON output",
)
def validate_contract(
    contract: str | None,
    offline: bool,
    job_id: int | None,
    environment: str | None,
    code_path: str | None,
    metadata_profile: str | None,
    runtime_run: str | None = None,
    historical_runs: str | None = None,
    json_output: bool = False,
) -> None:
    """Validate a pipeline contract (offline) or Databricks job (live).

    Examples:
      dpif validate --contract examples/customer_daily.yaml --offline
    """
    settings = get_settings()
    if settings.debug:
        setup_logging(level="DEBUG")
    try:
        if offline:
            if not contract:
                click.echo("Error: --contract is required for offline validation", err=True)
                sys.exit(2)
            _run_offline_validation(
                contract,
                environment,
                code_path,
                metadata_profile,
                runtime_run,
                historical_runs,
                json_output,
            )
        else:
            if job_id is None:
                click.echo("Error: --job-id is required for live validation", err=True)
                sys.exit(2)
            _run_live_validation(job_id, environment or "prod")
    except SystemExit:
        raise
    except ConfigurationError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(2)
    except Exception as e:  # crash -> non-zero
        logger.exception("Validation crashed: %s", e)
        click.echo(f"Validation crashed: {e}", err=True)
        sys.exit(1)


def _run_offline_validation(
    contract_path: str,
    environment: str | None,
    code_path: str | None,
    metadata_profile: str | None,
    runtime_run_path: str | None = None,
    historical_runs_path: str | None = None,
    json_output: bool = False,
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

    historical_objs: list[Any] = []
    if historical_runs_path:
        with open(historical_runs_path, encoding="utf-8") as hf:
            raw_h = (
                json.loads(hf.read())
                if historical_runs_path.endswith(".json")
                else yaml.safe_load(hf.read())
            )
        if isinstance(raw_h, list):
            historical_objs = raw_h

    context: dict[str, Any] = {
        "pipeline_name": contract.pipeline_name,
        "pipeline_contract": contract,
        "source": src,
        "data_profile": profile,
        "code_snippet": code_text,
        "code_filename": code_filename,
        "code_analysis": analysis,
        "cluster_config": cluster_config,
        "job_config": job_config,
        "assumptions": {"mode": "offline-fixture"},
        "rule_context": rule_context,
        "runtime_run": runtime_obj,
        "runtime_data": runtime_obj,
        "historical_runs": historical_objs,
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

    if json_output:
        payload = assessment.to_dict()
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
    if runtime_obj:
        _display_performance_section(runtime_obj, results)
        _display_correlation_section(correlations)
    _display_scalability_section(results, contract, profile, runtime_obj, historical_objs)
    _display_findings(results)
    _display_score(score, readiness)
    _display_final_readiness_section(assessment)
    # Exit 0: validation completed (even with FAIL findings).


def _run_live_validation(job_id: int, environment: str) -> None:
    click.echo(f"Running live validation for job {job_id} in {environment}")
    click.echo("Live mode needs Databricks credentials; Phase 2 covers offline mode only.")
    click.echo("Use --offline mode with a pipeline contract.")
    sys.exit(2)


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
    historical_runs: list[Any],
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


def main() -> None:
    """Main entry point for the dpif CLI."""
    cli()


if __name__ == "__main__":
    main()
