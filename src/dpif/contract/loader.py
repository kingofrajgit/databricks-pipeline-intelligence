"""Load a pipeline YAML contract into a validated PipelineContract."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from dpif.models import (
    IngestionMode,
    JdbcMetadata,
    PipelineContract,
    ReliabilityRules,
    ScalabilityRules,
    ScheduleRules,
    SchemaColumn,
    SchemaDefinition,
    SLARules,
    Source,
    SourceFormat,
    SourceType,
    StreamingMetadata,
    Target,
)


def _source_type(value: Any) -> SourceType:
    text = str(value or "adls").lower()
    try:
        return SourceType(text)
    except ValueError:
        return SourceType.OTHER


def _source_format(value: Any) -> SourceFormat:
    text = str(value or "unknown").lower()
    try:
        return SourceFormat(text)
    except ValueError:
        return SourceFormat.UNKNOWN


def _ingestion_mode(explicit: Any, processing_type: str) -> IngestionMode:
    for candidate in (explicit, processing_type):
        text = str(candidate or "").lower().replace("-", "_").replace(" ", "_")
        if not text:
            continue
        try:
            return IngestionMode(text)
        except ValueError:
            continue
    return IngestionMode.UNKNOWN


def _positive_or_none(value: Any) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _schema_definition(raw: Any) -> SchemaDefinition | None:
    if isinstance(raw, dict):
        raw = raw.get("columns", [])
    if not isinstance(raw, list) or not raw:
        return None
    columns = []
    for entry in raw:
        if isinstance(entry, dict) and entry.get("name"):
            columns.append(
                SchemaColumn(
                    name=str(entry.get("name")),
                    data_type=str(entry.get("data_type", entry.get("type", "unknown"))),
                    nullable=bool(entry.get("nullable", True)),
                )
            )
        elif isinstance(entry, str):
            columns.append(SchemaColumn(name=entry))
    return SchemaDefinition(columns=columns) if columns else None


def _jdbc_metadata(raw: Any) -> JdbcMetadata | None:
    if not isinstance(raw, dict) or not raw:
        return None
    fields = (
        "database_type",
        "query",
        "incremental_column",
        "partition_column",
        "lower_bound",
        "upper_bound",
        "num_partitions",
        "fetch_size",
    )
    if not any(raw.get(k) is not None for k in fields):
        return None
    return JdbcMetadata(
        database_type=raw.get("database_type"),
        query=raw.get("query"),
        incremental_column=raw.get("incremental_column"),
        partition_column=raw.get("partition_column"),
        lower_bound=raw.get("lower_bound"),
        upper_bound=raw.get("upper_bound"),
        num_partitions=raw.get("num_partitions"),
        fetch_size=raw.get("fetch_size"),
    )


def _streaming_metadata(raw: Any) -> StreamingMetadata | None:
    if not isinstance(raw, dict) or not raw:
        return None
    fields = ("partition_count", "checkpoint_location", "trigger", "watermark", "starting_position")
    if not any(raw.get(k) is not None for k in fields):
        return None
    return StreamingMetadata(
        partition_count=raw.get("partition_count"),
        checkpoint_location=raw.get("checkpoint_location"),
        trigger=raw.get("trigger"),
        watermark=raw.get("watermark"),
        starting_position=raw.get("starting_position"),
    )


def _timeout_minutes(reliability_d: dict, job_d: dict) -> float | None:
    if reliability_d.get("timeout_minutes") is not None:
        return float(reliability_d["timeout_minutes"])
    if job_d.get("timeout_seconds"):
        return float(job_d["timeout_seconds"]) / 60.0
    return None


def load_contract_file(path: str | Path) -> PipelineContract:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Contract file not found: {path}")
    with open(p, encoding="utf-8") as f:
        data: dict[str, Any] = yaml.safe_load(f) or {}

    pipeline = data.get("pipeline", {}) or {}
    source_d = data.get("source", {}) or {}
    processing_d = data.get("processing", {}) or {}
    target_d = data.get("target", {}) or {}
    sla_d = data.get("sla", {}) or {}
    schedule_d = data.get("schedule", {}) or {}
    reliability_d = data.get("reliability", {}) or {}
    scalability_d = data.get("scalability", {}) or {}
    cluster_d = data.get("cluster", {}) or {}
    job_d = data.get("job", {}) or {}

    processing_type = str(processing_d.get("type", data.get("processing", "batch")))
    expected_gb = _positive_or_none(
        source_d.get("expected_daily_volume_gb", data.get("expected_daily_volume_gb"))
    )
    peak_gb = _positive_or_none(
        source_d.get("peak_daily_volume_gb", data.get("peak_daily_volume_gb"))
    )
    growth = _positive_or_none(source_d.get("growth_rate_percent", data.get("growth_rate_percent")))
    partitioning = source_d.get("partitioning", source_d.get("partition_keys", []) or [])
    source = Source(
        source_id=f"{pipeline.get('name', 'pipeline')}-source",
        name=f"{pipeline.get('name', 'pipeline')}-source",
        type=_source_type(source_d.get("type", "adls")),
        config={
            k: v
            for k, v in source_d.items()
            if k
            not in (
                "type",
                "format",
                "path",
                "location",
                "partitioning",
                "partition_keys",
                "schema",
                "columns",
                "jdbc",
                "streaming",
                "ingestion",
                "compression",
                "expected_daily_volume_gb",
                "peak_daily_volume_gb",
                "growth_rate_percent",
            )
        },
        path=str(source_d.get("path", source_d.get("location", ""))),
        location=str(source_d.get("location", source_d.get("path", ""))),
        format=_source_format(source_d.get("format", "unknown")),
        ingestion_mode=_ingestion_mode(
            (source_d.get("ingestion") or {}).get("mode")
            if isinstance(source_d.get("ingestion"), dict)
            else source_d.get("ingestion_mode"),
            processing_type,
        ),
        schema_definition=_schema_definition(source_d.get("schema", source_d.get("columns"))),
        expected_volume_gb=expected_gb,
        peak_volume_gb=peak_gb,
        growth_rate_percent=growth,
        partitioning=[str(p) for p in partitioning] if isinstance(partitioning, list) else [],
        compression=str(source_d.get("compression", "")),
        jdbc=_jdbc_metadata(source_d.get("jdbc")),
        streaming=_streaming_metadata(source_d.get("streaming")),
    )
    target = Target(
        target_id=f"{pipeline.get('name', 'pipeline')}-target",
        type=str(target_d.get("type", "delta")),
        catalog=str(target_d.get("catalog", "")),
        schema=str(target_d.get("schema", target_d.get("schema_name", ""))),
        path=str(target_d.get("path", "")),
        format=str(target_d.get("format", "delta")),
    )
    contract = PipelineContract(
        contract_id=f"{pipeline.get('name', 'pipeline')}-contract",
        pipeline_name=str(pipeline.get("name", "unknown")),
        environment=str(pipeline.get("environment", "production")),
        owner=str(pipeline.get("owner", "")),
        source=source,
        processing=processing_type,
        language=str(processing_d.get("language", "pyspark")),
        target=target,
        sla=SLARules(
            max_runtime_minutes=float(sla_d.get("max_runtime_minutes", 60)),
            warning_runtime_minutes=float(
                sla_d.get("warning_runtime_minutes", sla_d.get("max_runtime_minutes", 60))
            ),
        ),
        schedule=ScheduleRules(
            frequency=str(schedule_d.get("frequency", "daily")),
            time=str(schedule_d.get("time", "02:00")),
            timezone=str(schedule_d.get("timezone", "UTC")),
        ),
        reliability=ReliabilityRules(
            retry_count=int(reliability_d.get("retry_count", job_d.get("max_retries", 2))),
            idempotent=bool(reliability_d.get("idempotent", True)),
            retry_safety=bool(reliability_d.get("retry_safety", True)),
            timeout_minutes=_timeout_minutes(reliability_d, job_d),
        ),
        scalability=ScalabilityRules(
            forecast_horizon_days=int(scalability_d.get("forecast_horizon_days", 365)),
            expected_growth_percent_per_day=float(
                scalability_d.get("expected_growth_percent_per_day", 0.0)
            ),
            peak_forecast_gb_per_day=float(scalability_d.get("peak_forecast_gb_per_day", 0.0)),
        ),
        expected_daily_volume_gb=float(
            source_d.get("expected_daily_volume_gb", data.get("expected_daily_volume_gb", 0.0))
        ),
        peak_daily_volume_gb=float(
            source_d.get("peak_daily_volume_gb", data.get("peak_daily_volume_gb", 0.0))
        ),
        growth_rate_percent=float(
            source_d.get("growth_rate_percent", data.get("growth_rate_percent", 0.0))
        ),
    )
    # stash raw cluster/job sections for offline validation use
    object.__setattr__(contract, "_cluster_raw", cluster_d)
    object.__setattr__(contract, "_job_raw", job_d)
    return contract


def contract_cluster_job(contract: PipelineContract) -> tuple[dict[str, Any], dict[str, Any]]:
    cluster = dict(getattr(contract, "_cluster_raw", {}) or {})
    job = dict(getattr(contract, "_job_raw", {}) or {})
    if not job:
        job = {
            "max_retries": contract.reliability.retry_count,
            "timeout_seconds": int((contract.reliability.timeout_minutes or 60) * 60),
        }
    return cluster, job
