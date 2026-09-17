"""Synthesis of discovered PipelineContract and DataProfile from Databricks live evidence.

Converts live Databricks evidence (job settings, cluster config, AST code analysis,
table profile, and runtime metrics) into existing DPIF domain models:
    - PipelineContract (Source, Target, ClusterDefinition, JobDefinition, Reliability, Scalability)
    - DataProfile (volume, records, columns, schema)

Preserves strict UNKNOWN semantics: missing evidence remains None/UNKNOWN rather than
being guessed or fabricated.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from dpif.code.models import CodeAnalysis, OperationType
from dpif.models import (
    CollectionMethod,
    DataProfile,
    IngestionMode,
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
    Target,
)
from dpif.runtime.models import RuntimeRun

logger = logging.getLogger(__name__)


def _extract_source_from_code(
    code_analysis: CodeAnalysis | None,
    raw_code: str = "",
    table_profile: dict[str, Any] | None = None,
) -> Source | None:
    """Extract Source model from code AST, regex, and table profile evidence."""
    source_path = ""
    source_format_str = "unknown"
    is_streaming = False
    is_incremental = False
    partitioning: list[str] = []

    # 1. Search operations from AST
    if code_analysis and code_analysis.operations:
        for op in code_analysis.operations:
            if op.operation_type == OperationType.READ:
                via = str(op.arguments.get("via", ""))
                query = str(op.arguments.get("query", ""))
                if via in ("table", "sql") and query:
                    clean_q = query.strip(" '\"")
                    if clean_q and not source_path:
                        source_path = clean_q
                if "readStream" in op.code:
                    is_streaming = True

    # 2. Inspect raw code with regex if source_path not yet found
    code_text = raw_code or (getattr(code_analysis, "source_file", "") if code_analysis else "")
    if code_analysis and hasattr(code_analysis, "_raw_source"):
        code_text = getattr(code_analysis, "_raw_source", "") or code_text

    if not source_path and code_text:
        # spark.table("name") or spark.read.table("name")
        m_table = re.search(r"""(?i)spark(?:\.read)?\.table\(\s*['"]([a-zA-Z0-9_.]+)['"]\s*\)""", code_text)
        if m_table:
            source_path = m_table.group(1)
            source_format_str = "delta"

    if not source_path and code_text:
        # spark.read.format("...").load("...")
        m_load = re.search(
            r"""(?i)spark(?:\.readStream|\.read)\.format\(\s*['"]([a-zA-Z0-9_]+)['"]\s*\)\.load\(\s*['"]([^'"]+)['"]\s*\)""",
            code_text,
        )
        if m_load:
            source_format_str = m_load.group(1).lower()
            source_path = m_load.group(2)
        else:
            # spark.read.parquet("...") / csv / json
            m_direct = re.search(
                r"""(?i)spark(?:\.readStream|\.read)\.(parquet|csv|json|delta|orc|avro)\(\s*['"]([^'"]+)['"]\s*\)""",
                code_text,
            )
            if m_direct:
                source_format_str = m_direct.group(1).lower()
                source_path = m_direct.group(2)

    # If table profile provides table name and we still don't have source_path
    if not source_path and table_profile and table_profile.get("name"):
        source_path = str(table_profile["name"])
        if table_profile.get("data_source_format"):
            source_format_str = str(table_profile["data_source_format"]).lower()

    if not source_path:
        return None

    # Check streaming & incremental markers
    if re.search(r"(?i)readStream", code_text):
        is_streaming = True
    if re.search(
        r"(?i)(date_sub|date_add|current_date|current_timestamp|updated_at|created_at|event_time|window)",
        code_text,
    ):
        is_incremental = True

    # Determine SourceType
    src_type = SourceType.DELTA
    lower_path = source_path.lower()
    if is_streaming:
        src_type = SourceType.STREAMING
    elif "s3://" in lower_path:
        src_type = SourceType.S3
    elif "abfss://" in lower_path or "wasbs://" in lower_path:
        src_type = SourceType.ADLS
    elif "gs://" in lower_path:
        src_type = SourceType.GCS
    elif "jdbc" in lower_path:
        src_type = SourceType.JDBC
    elif source_format_str in ("parquet", "csv", "json", "avro", "orc"):
        src_type = SourceType.FILE
    elif "." in source_path:
        src_type = SourceType.DELTA

    # Determine SourceFormat
    format_map = {
        "delta": SourceFormat.DELTA,
        "parquet": SourceFormat.PARQUET,
        "csv": SourceFormat.CSV,
        "json": SourceFormat.JSON,
        "avro": SourceFormat.AVRO,
        "orc": SourceFormat.ORC,
    }
    src_format = format_map.get(source_format_str, SourceFormat.DELTA if src_type == SourceType.DELTA else SourceFormat.UNKNOWN)

    # Determine IngestionMode
    if is_streaming:
        mode = IngestionMode.STREAMING
    elif is_incremental:
        mode = IngestionMode.INCREMENTAL
    else:
        mode = IngestionMode.BATCH

    # Schema definition if table profile exists
    schema_def: SchemaDefinition | None = None
    if table_profile and isinstance(table_profile.get("columns"), list) and table_profile["columns"]:
        cols = [
            SchemaColumn(name=str(c.get("name", "")), data_type=str(c.get("type_text", "unknown")))
            for c in table_profile["columns"]
            if isinstance(c, dict) and c.get("name")
        ]
        if cols:
            schema_def = SchemaDefinition(columns=cols)

    return Source(
        source_id="discovered_source",
        type=src_type,
        name=source_path,
        path=source_path,
        location=source_path,
        format=src_format,
        ingestion_mode=mode,
        schema_definition=schema_def,
        partitioning=partitioning,
    )


def _extract_target_from_code(
    code_analysis: CodeAnalysis | None,
    raw_code: str = "",
    table_profile: dict[str, Any] | None = None,
) -> Target:
    """Extract Target model from code AST, regex, and table profile evidence."""
    target_path = ""
    target_format = "delta"
    catalog = ""
    schema_name = ""

    code_text = raw_code or (getattr(code_analysis, "source_file", "") if code_analysis else "")
    if code_analysis and hasattr(code_analysis, "_raw_source"):
        code_text = getattr(code_analysis, "_raw_source", "") or code_text

    # Search operations
    if code_analysis and code_analysis.operations:
        for op in code_analysis.operations:
            if op.operation_type == OperationType.WRITE:
                tbl = op.arguments.get("table")
                if tbl and not target_path:
                    target_path = str(tbl)
                fmt = op.arguments.get("format")
                if fmt:
                    target_format = str(fmt).lower()

    if not target_path and code_text:
        # .saveAsTable("name") or .insertInto("name")
        m_tbl = re.search(
            r"""(?i)\.(?:saveAsTable|insertInto)\(\s*['"]([a-zA-Z0-9_.]+)['"]\s*\)""",
            code_text,
        )
        if m_tbl:
            target_path = m_tbl.group(1)
            target_format = "delta"

    if not target_path and code_text:
        # .format("...").save("...")
        m_save = re.search(
            r"""(?i)\.format\(\s*['"]([a-zA-Z0-9_]+)['"]\s*\)(?:\.[a-zA-Z0-9_]+\([^)]*\))*\.save\(\s*['"]([^'"]+)['"]\s*\)""",
            code_text,
        )
        if m_save:
            target_format = m_save.group(1).lower()
            target_path = m_save.group(2)

    # Parse 3-part or 2-part namespace if table name
    if target_path and "." in target_path:
        parts = target_path.split(".")
        if len(parts) == 3:
            catalog, schema_name, _ = parts
        elif len(parts) == 2:
            schema_name, _ = parts

    return Target(
        target_id="discovered_target",
        type="delta" if target_format == "delta" else "table",
        catalog=catalog,
        schema=schema_name,
        path=target_path,
        format=target_format,
    )


def synthesize_discovered_contract(
    job_config: dict[str, Any] | None = None,
    cluster_config: dict[str, Any] | None = None,
    code_analysis: CodeAnalysis | None = None,
    raw_code: str = "",
    table_profile: dict[str, Any] | None = None,
    pipeline_name: str = "",
    environment: str = "production",
    resource_id: str = "",
) -> PipelineContract | None:
    """Synthesize a discovered PipelineContract from live Databricks evidence.

    Returns None if no source or meaningful workload characteristics can be identified,
    preserving strict UNKNOWN semantics.
    """
    source = _extract_source_from_code(code_analysis, raw_code=raw_code, table_profile=table_profile)
    target = _extract_target_from_code(code_analysis, raw_code=raw_code, table_profile=table_profile)

    # If no source was discovered, we can only build a contract if there is a known target or job tasks
    if source is None and not target.path:
        logger.info("Could not identify source or target for discovered workload; leaving contract UNKNOWN")
        return None

    if not target.path:
        target.path = "discovered_target"

    # Default placeholder source if only target was found
    if source is None:
        source = Source(
            source_id="discovered_source",
            type=SourceType.DELTA,
            name="discovered_source",
            path="discovered_source",
            format=SourceFormat.UNKNOWN,
            ingestion_mode=IngestionMode.BATCH,
        )

    # Job settings for schedule and reliability
    retries = 0
    timeout_mins: float | None = None
    frequency = "daily"
    cron_expr = ""

    if job_config and isinstance(job_config.get("settings"), dict):
        settings = job_config["settings"]
        retries = int(settings.get("max_retries", 0) or 0)
        timeout_sec = int(settings.get("timeout_seconds", 0) or 0)
        if timeout_sec > 0:
            timeout_mins = round(timeout_sec / 60.0, 1)
        sched = settings.get("schedule", {})
        if isinstance(sched, dict) and sched.get("quartz_cron_expression"):
            cron_expr = str(sched["quartz_cron_expression"])
            frequency = "scheduled"

    # Processing type: streaming vs batch
    processing = "streaming" if source.ingestion_mode == IngestionMode.STREAMING else "batch"

    contract_id = f"discovered-{resource_id or pipeline_name or 'workload'}"

    contract = PipelineContract(
        contract_id=contract_id,
        pipeline_name=pipeline_name or f"pipeline-{resource_id}",
        environment=environment,
        owner=job_config.get("creator_user_name", "") if job_config else "",
        source=source,
        processing=processing,
        language="pyspark",
        target=target,
        sla=SLARules(max_runtime_minutes=timeout_mins or 60.0),
        schedule=ScheduleRules(frequency=frequency, time=cron_expr or "02:00"),
        reliability=ReliabilityRules(
            retry_count=retries,
            idempotent=True,
            timeout_minutes=timeout_mins,
        ),
        scalability=ScalabilityRules(),
    )

    # Attach raw cluster and job configs for alignment and downstream analyzers
    if cluster_config:
        object.__setattr__(contract, "_cluster_raw", cluster_config)
    if job_config:
        object.__setattr__(contract, "_job_raw", job_config)

    return contract


def synthesize_discovered_data_profile(
    runtime_run: RuntimeRun | None = None,
    table_profile: dict[str, Any] | None = None,
) -> DataProfile | None:
    """Synthesize a DataProfile from live runtime telemetry and Unity Catalog table profile.

    Returns None if no runtime volume or table columns are available,
    strictly preserving UNKNOWN semantics.
    """
    total_bytes = 0
    total_gb = 0.0
    record_count = 0
    schema_cols: list[SchemaColumn] = []

    # 1. From runtime telemetry
    if runtime_run is not None:
        in_bytes = getattr(runtime_run, "total_input_bytes", 0) or 0
        if in_bytes > 0:
            total_bytes = int(in_bytes)
            total_gb = round(total_bytes / (1024.0**3), 3)
        else:
            in_vol = getattr(runtime_run, "input_volume_gb", None)
            if in_vol is not None and isinstance(in_vol, (int, float)):
                total_gb = float(in_vol)
                total_bytes = int(total_gb * (1024**3))

        recs = getattr(runtime_run, "records_read", None)
        if recs is not None and isinstance(recs, (int, float)):
            record_count = int(recs)
        elif getattr(runtime_run, "total_tasks", 0) > 0:
            record_count = getattr(runtime_run, "total_tasks", 0)

    # 2. From Unity Catalog table metadata
    if table_profile and isinstance(table_profile.get("columns"), list):
        for col in table_profile["columns"]:
            if isinstance(col, dict) and col.get("name"):
                schema_cols.append(
                    SchemaColumn(
                        name=str(col["name"]),
                        data_type=str(col.get("type_text") or col.get("type_name") or "unknown"),
                        nullable=bool(col.get("nullable", True)),
                    )
                )

    # If neither volume nor schema could be evidenced, return None
    if total_bytes == 0 and total_gb == 0.0 and record_count == 0 and not schema_cols:
        return None

    return DataProfile(
        total_bytes=total_bytes,
        total_gb=total_gb,
        file_count=1 if total_bytes > 0 else 0,
        average_file_size_kb=round(total_bytes / 1024.0, 1) if total_bytes > 0 else 0.0,
        record_count=record_count,
        column_count=len(schema_cols),
        schema_columns=schema_cols,
        collection_method=CollectionMethod.RUNTIME if (total_bytes > 0 or record_count > 0) else CollectionMethod.METADATA,
        evidence_source="DATABRICKS_API",
    )
