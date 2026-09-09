from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Severity(StrEnum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class CheckpointStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNKNOWN = "UNKNOWN"


class AnalysisMethod(StrEnum):
    METADATA = "metadata"
    SAMPLE = "sample"
    FULL = "full"
    UNAVAILABLE = "unavailable"


class SourceType(StrEnum):
    ADLS = "adls"
    S3 = "s3"
    GCS = "gcs"
    LOCAL_FILE = "local_file"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    JDBC = "jdbc"
    REST_API = "rest_api"
    KAFKA = "kafka"
    EVENT_HUB = "event_hub"
    STREAMING = "streaming"
    DELTA = "delta"
    FILE = "file"
    OTHER = "other"


class SourceFormat(StrEnum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    DELTA = "delta"
    UNKNOWN = "unknown"


class IngestionMode(StrEnum):
    BATCH = "batch"
    INCREMENTAL = "incremental"
    CDC = "cdc"
    STREAMING = "streaming"
    MICRO_BATCH = "micro_batch"
    FULL_LOAD = "full_load"
    UNKNOWN = "unknown"


class CollectionMethod(StrEnum):
    METADATA = "metadata"
    SAMPLE = "sample"
    FULL_SCAN = "full_scan"
    RUNTIME = "runtime"
    FIXTURE = "fixture"
    UNKNOWN = "unknown"


class PipelineProcessingType(StrEnum):
    FULL_LOAD = "FULL_LOAD"
    INCREMENTAL = "INCREMENTAL"
    CDC = "CDC"
    BATCH = "BATCH"
    STREAMING = "STREAMING"
    MICRO_BATCH = "MICRO_BATCH"


# ---------------------------------------------------------------------------
# EvidenceRecord
# ---------------------------------------------------------------------------


class EvidenceRecord(BaseModel):
    """Structured evidence supporting a finding."""

    rule_id: str
    status: CheckpointStatus
    severity: Severity
    observed: dict[str, Any] = Field(default_factory=dict)
    expected: dict[str, Any] = Field(default_factory=dict)
    evidence: list[str] = Field(default_factory=list)
    recommendation: str = ""
    confidence: float = 0.0
    method: AnalysisMethod = AnalysisMethod.UNAVAILABLE

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "status": self.status.value,
            "severity": self.severity.value,
            "observed": self.observed,
            "expected": self.expected,
            "evidence": self.evidence,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "method": self.method.value,
        }


# ---------------------------------------------------------------------------
# Finding
# ---------------------------------------------------------------------------


class Finding(BaseModel):
    """A single finding/rule result with evidence.

    Required structured fields (§11): rule_id, category, severity, status,
    title, description, evidence, recommendation, confidence, blocking.
    ``name`` is kept as an alias of ``title`` for backwards compatibility.
    """

    finding_id: str
    rule_id: str
    name: str
    category: str
    status: CheckpointStatus
    severity: Severity
    pipeline_name: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    evidence: EvidenceRecord
    assumptions: dict[str, Any] = Field(default_factory=dict)
    title: str = ""
    description: str = ""
    recommendation: str = ""
    confidence: float = 0.0
    blocking: bool = False

    def to_dict(self) -> dict[str, Any]:
        title = self.title or self.name
        return {
            "finding_id": self.finding_id,
            "rule_id": self.rule_id,
            "name": self.name,
            "title": title,
            "category": self.category,
            "status": self.status.value,
            "severity": self.severity.value,
            "description": self.description,
            "pipeline_name": self.pipeline_name,
            "timestamp": self.timestamp.isoformat(),
            "evidence": self.evidence.to_dict(),
            "recommendation": self.recommendation or self.evidence.recommendation,
            "confidence": self.confidence or self.evidence.confidence,
            "blocking": self.blocking,
            "assumptions": self.assumptions,
        }


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


class Checkpoint(BaseModel):
    """A first-class checkpoint with status, severity, score, and evidence."""

    checkpoint_id: str
    name: str
    category: str
    status: CheckpointStatus = CheckpointStatus.UNKNOWN
    severity: Severity = Severity.INFO
    score: float = 0.0  # 0.0 - 1.0
    evidence: EvidenceRecord = Field(
        default_factory=lambda: EvidenceRecord(
            rule_id="unknown",
            status=CheckpointStatus.UNKNOWN,
            severity=Severity.INFO,
        )
    )
    findings: list[Finding] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    assumptions: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)  # checkpoint_ids this depends on
    parent_checkpoint: str | None = None  # id of parent checkpoint

    def validate_dependencies(self, checkpoints: dict[str, Checkpoint]) -> bool:
        """Check if all dependent checkpoints pass or are not applicable."""
        for dep_id in self.depends_on:
            if dep_id not in checkpoints:
                return False
            dep = checkpoints[dep_id]
            # If dependency is FAIL, we cannot pass
            if dep.status == CheckpointStatus.FAIL:
                return False
            # If dependency is UNKNOWN, we default to UNKNOWN (not PASS)
            if dep.status == CheckpointStatus.UNKNOWN:
                return False
        return True

    def compute_status(self, checkpoints: dict[str, Checkpoint]) -> CheckpointStatus:
        """Compute status based on dependencies and own evaluation."""
        if self.depends_on:
            if not self.validate_dependencies(checkpoints):
                return CheckpointStatus.UNKNOWN
        return self.status

    def to_dict(self) -> dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "name": self.name,
            "category": self.category,
            "status": self.status.value,
            "severity": self.severity.value,
            "score": self.score,
            "evidence": self.evidence.to_dict(),
            "findings": [f.to_dict() for f in self.findings],
            "timestamp": self.timestamp.isoformat(),
            "assumptions": self.assumptions,
            "depends_on": self.depends_on,
        }


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------


class Rule(BaseModel):
    """A rule for static/code analysis."""

    rule_id: str
    name: str
    category: str
    description: str
    severity: Severity
    condition: str  # Pattern or SQL expression
    evidence_required: list[str] = Field(default_factory=list)
    recommendation: str = ""
    blocking: bool = False
    score: float = 0.0  # Impact on checkpoint score (0.0 - 1.0)
    version: str = "1.0.0"
    context_aware: bool = True
    requires_context: list[str] = Field(default_factory=list)
    # Optional analyzer entry point for non-regex rules, e.g. "small_files".
    # When set, the checkpoint engine routes evaluation to the registered
    # analyzer instead of executing ``condition`` as a regex.
    evaluator: str = ""
    # Rule-specific tunable thresholds (analyzers must not hard-code them).
    params: dict[str, Any] = Field(default_factory=dict)
    # Optional status ceiling, e.g. "WARN" for rules that must never FAIL
    # (static analysis cannot prove the underlying runtime claim).
    cap_status: str = ""

    def evaluate(
        self, data: dict[str, Any], context: dict[str, Any] | None = None
    ) -> Finding | None:
        """Evaluate the rule against data and return a Finding or None.

        Context-aware: without data/cluster context the finding is a
        qualified warning (never invented evidence). Callers may pass
        context either nested under ``data['context']`` or as ``context``.
        Rules with an ``evaluator`` delegate to the registered analyzer.
        """
        if self.evaluator:
            if self.rule_id.startswith("CODE-SQL-"):
                from dpif.sql import evaluators as sql_evaluators

                fn = sql_evaluators.resolve(self.evaluator)
            elif self.rule_id.startswith("CONFIG-"):
                from dpif.discovery import evaluators as config_evaluators

                fn = config_evaluators.resolve(self.evaluator)
            elif self.rule_id.startswith("RUNTIME-"):
                from dpif.runtime import evaluators as runtime_evaluators

                fn = runtime_evaluators.resolve(self.evaluator)
            elif self.rule_id.startswith("SCALABILITY-"):
                from dpif.scalability import evaluators as scalability_evaluators

                fn = scalability_evaluators.resolve(self.evaluator)
            else:
                from dpif.analyzers.data import evaluators as data_evaluators

                fn = data_evaluators.resolve(self.evaluator)
                if fn is None:
                    from dpif.sql import evaluators as sql_evaluators

                    fn = sql_evaluators.resolve(self.evaluator)
                if fn is None:
                    from dpif.discovery import evaluators as config_evaluators

                    fn = config_evaluators.resolve(self.evaluator)
                if fn is None:
                    from dpif.runtime import evaluators as runtime_evaluators

                    fn = runtime_evaluators.resolve(self.evaluator)
            if fn is None:
                return None
            return fn(self, data, context or {})
        import re

        pattern = self.condition
        try:
            matched = bool(
                re.search(pattern, data.get("text", ""), re.IGNORECASE)
                if isinstance(pattern, str) and pattern
                else False
            )
        except re.error:
            return None

        if not matched:
            return None

        # Extract evidence from the matched data
        evidence_locations = data.get("evidence", [])

        # Build recommendation
        rec = self.recommendation or f"Avoid: {self.description}"

        # Determine observed/expected
        observed = data.get("observed", {})
        expected = data.get("expected", {})

        # Context qualification (no invented metrics).
        ctx: dict[str, Any] = {}
        nested = data.get("context")
        if isinstance(nested, dict):
            ctx.update(nested)
        if context:
            ctx.update(context)
        status = CheckpointStatus.FAIL
        confidence = 0.9
        assumptions = dict(data.get("assumptions", {}) or {})
        if self.context_aware:
            known = (
                "data_size_gb",
                "row_count",
                "table_size_gb",
                "cluster_size",
                "cluster_workers",
                "workload_type",
                "runtime_minutes",
                "historical_runs",
                "source_type",
            )
            relevant = [k for k in known if k in ctx]
            if self.requires_context and not any(k in ctx for k in self.requires_context):
                status = CheckpointStatus.WARN
                confidence = 0.5
                assumptions["context-unavailable"] = (
                    f"Rule {self.rule_id} requires {self.requires_context} but none was provided."
                )
            elif not relevant:
                status = CheckpointStatus.WARN if not self.blocking else CheckpointStatus.FAIL
                confidence = 0.5 if not self.blocking else 0.55
                assumptions["context-unavailable"] = (
                    "No data/cluster context provided; syntax-level finding only."
                )
            else:
                size_gb = ctx.get("data_size_gb", ctx.get("table_size_gb"))
                if isinstance(size_gb, (int, float)) and size_gb < 1.0:
                    status = CheckpointStatus.WARN
                    confidence = 0.6
                    assumptions["small-data"] = f"Observed data size {size_gb} GB < 1.0 GB."
            if relevant:
                assumptions.setdefault("context", {k: v for k, v in ctx.items() if k in known})

        finding = Finding(
            finding_id=f"{self.rule_id}-{self.category}",
            rule_id=self.rule_id,
            name=self.name,
            title=self.name,
            description=self.description,
            category=self.category,
            status=status,
            severity=self.severity,
            pipeline_name=data.get("pipeline_name", "unknown"),
            timestamp=datetime.now(UTC),
            evidence=EvidenceRecord(
                rule_id=self.rule_id,
                status=status,
                severity=self.severity,
                observed=observed,
                expected=expected,
                evidence=evidence_locations,
                recommendation=rec,
                confidence=confidence,
            ),
            recommendation=rec,
            confidence=confidence,
            blocking=self.blocking,
            assumptions=assumptions,
        )
        return finding

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "category": self.category,
            "description": self.description,
            "severity": self.severity.value,
            "condition": self.condition,
            "evidence_required": self.evidence_required,
            "recommendation": self.recommendation,
            "blocking": self.blocking,
            "score": self.score,
            "version": self.version,
            "context_aware": self.context_aware,
            "requires_context": self.requires_context,
            "evaluator": self.evaluator,
            "params": self.params,
            "cap_status": self.cap_status,
        }


# ---------------------------------------------------------------------------
# Source metadata (JDBC / streaming / schema)
# ---------------------------------------------------------------------------


class JdbcMetadata(BaseModel):
    """Parallel-extraction metadata for JDBC sources. All fields optional."""

    database_type: str | None = None
    query: str | None = None
    incremental_column: str | None = None
    partition_column: str | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None
    num_partitions: int | None = None
    fetch_size: int | None = None

    def has_parallel_evidence(self) -> bool:
        """True when sharded/parallel extraction is documented."""
        return bool(self.partition_column and (self.num_partitions or 0) > 1)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class StreamingMetadata(BaseModel):
    """Streaming source configuration. Runtime fields stay None offline."""

    partition_count: int | None = None
    checkpoint_location: str | None = None
    trigger: str | None = None
    watermark: str | None = None
    starting_position: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class SchemaColumn(BaseModel):
    """A single observed/expected column."""

    name: str
    data_type: str = "unknown"
    nullable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class SchemaDefinition(BaseModel):
    """An explicit column-level schema for drift comparison."""

    columns: list[SchemaColumn] = Field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def type_map(self) -> dict[str, str]:
        return {c.name: c.data_type for c in self.columns}

    def to_dict(self) -> dict[str, Any]:
        return {"columns": [c.to_dict() for c in self.columns]}


class EvidenceCoverage(BaseModel):
    """How much of the validation was actually evidenced (not a score).

    Tier breakdown (Phase 4): static = syntax/structure only,
    data-context = code/data/source evidence combined, runtime = measured
    runs. Static analysis never counts as runtime verification.
    """

    total_checks: int = 0
    evaluated_checks: int = 0
    unknown_checks: int = 0
    coverage_percentage: float = 0.0
    static_checks: int = 0
    context_checks: int = 0
    runtime_checks: int = 0

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


class Source(BaseModel):
    """Source configuration and profile.

    Volume fields are ``None`` when unconfigured — never confuse a missing
    configuration with a valid zero value.
    """

    source_id: str
    type: SourceType
    name: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    data_profile: DataProfile | None = None
    path: str = ""
    format: SourceFormat = SourceFormat.UNKNOWN
    location: str = ""
    ingestion_mode: IngestionMode = IngestionMode.UNKNOWN
    schema_definition: SchemaDefinition | None = None
    expected_volume_gb: float | None = None
    peak_volume_gb: float | None = None
    growth_rate_percent: float | None = None
    partitioning: list[str] = Field(default_factory=list)
    compression: str = ""
    jdbc: JdbcMetadata | None = None
    streaming: StreamingMetadata | None = None
    connection_string: str = ""  # masked in reports

    @property
    def is_cloud(self) -> bool:
        return self.type in (SourceType.ADLS, SourceType.S3, SourceType.GCS)

    @property
    def is_local(self) -> bool:
        return self.type in (SourceType.LOCAL_FILE, SourceType.FILE)

    @property
    def reference(self) -> str:
        """Human location/reference: explicit location, else path, else config."""
        if self.location:
            return self.location
        if self.path:
            return self.path
        loc = self.config.get("location") or self.config.get("path") or ""
        return str(loc)

    def to_dict(self) -> dict[str, Any]:
        fmt = self.format.value if isinstance(self.format, SourceFormat) else str(self.format)
        mode = (
            self.ingestion_mode.value
            if isinstance(self.ingestion_mode, IngestionMode)
            else str(self.ingestion_mode)
        )
        return {
            "source_id": self.source_id,
            "name": self.name,
            "type": self.type.value,
            "config": self.config,
            "path": self.path,
            "location": self.reference,
            "format": fmt,
            "ingestion_mode": mode,
            "expected_volume_gb": self.expected_volume_gb,
            "peak_volume_gb": self.peak_volume_gb,
            "growth_rate_percent": self.growth_rate_percent,
            "partitioning": self.partitioning,
            "compression": self.compression,
            "jdbc": self.jdbc.to_dict() if self.jdbc else None,
            "streaming": self.streaming.to_dict() if self.streaming else None,
        }


# ---------------------------------------------------------------------------
# DataProfile
# ---------------------------------------------------------------------------


class DataProfile(BaseModel):
    """Data profile collected from source."""

    model_config = ConfigDict(populate_by_name=True)

    total_bytes: int = 0
    total_gb: float = 0.0
    file_count: int = 0
    average_file_size_kb: float = 0.0
    median_file_size_kb: float = 0.0
    p95_file_size_kb: float = 0.0
    p99_file_size_kb: float = 0.0
    min_file_size_kb: float = 0.0
    max_file_size_kb: float = 0.0
    record_count: int = 0
    partition_count: int = 0
    partition_distribution: dict[str, Any] = Field(default_factory=dict)
    # ``schema`` shadows BaseModel.schema, so the field lives as
    # ``schema_text`` with ``schema`` kept as a population alias.
    schema_text: str = Field(default="", alias="schema")
    column_count: int = 0
    null_distribution: dict[str, int] = Field(default_factory=dict)
    duplicate_indicators: list[str] = Field(default_factory=list)
    analysis_method: AnalysisMethod = AnalysisMethod.UNAVAILABLE
    # Phase 3: authoritative evidence provenance. FIXTURE profiles must
    # never be represented as runtime scans.
    collection_method: CollectionMethod = CollectionMethod.UNKNOWN
    evidence_source: str = ""
    compression: str = ""
    partition_sizes_gb: dict[str, float] = Field(default_factory=dict)
    partition_record_counts: dict[str, int] = Field(default_factory=dict)
    schema_columns: list[SchemaColumn] = Field(default_factory=list)

    @property
    def has_sufficient_metadata(self) -> bool:
        return self.total_bytes > 0 and self.file_count > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_bytes": self.total_bytes,
            "total_gb": self.total_gb,
            "file_count": self.file_count,
            "average_file_size_kb": self.average_file_size_kb,
            "median_file_size_kb": self.median_file_size_kb,
            "p95_file_size_kb": self.p95_file_size_kb,
            "p99_file_size_kb": self.p99_file_size_kb,
            "min_file_size_kb": self.min_file_size_kb,
            "max_file_size_kb": self.max_file_size_kb,
            "record_count": self.record_count,
            "partition_count": self.partition_count,
            "partition_distribution": self.partition_distribution,
            "schema": self.schema_text,
            "column_count": self.column_count,
            "null_distribution": self.null_distribution,
            "duplicate_indicators": self.duplicate_indicators,
            "analysis_method": self.analysis_method.value,
            "collection_method": self.collection_method.value,
            "evidence_source": self.evidence_source,
            "compression": self.compression,
            "partition_sizes_gb": self.partition_sizes_gb,
            "partition_record_counts": self.partition_record_counts,
            "schema_columns": [c.to_dict() for c in self.schema_columns],
        }


# ---------------------------------------------------------------------------
# Target
# ---------------------------------------------------------------------------


class Target(BaseModel):
    """Target configuration."""

    model_config = ConfigDict(populate_by_name=True)

    target_id: str
    type: str = "delta"  # delta, table, view
    catalog: str = ""
    schema_text: str = Field(default="", alias="schema")
    path: str = ""
    format: str = "parquet"

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_id": self.target_id,
            "type": self.type,
            "catalog": self.catalog,
            "schema": self.schema_text,
            "path": self.path,
            "format": self.format,
        }


# ---------------------------------------------------------------------------
# SLA Rules
# ---------------------------------------------------------------------------


class SLARules(BaseModel):
    max_runtime_minutes: float = 60.0
    warning_runtime_minutes: float = 80.0

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


# ---------------------------------------------------------------------------
# Schedule Rules
# ---------------------------------------------------------------------------


class ScheduleRules(BaseModel):
    frequency: str = "daily"
    time: str = "02:00"
    timezone: str = "UTC"

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


# ---------------------------------------------------------------------------
# Reliability Rules
# ---------------------------------------------------------------------------


class ReliabilityRules(BaseModel):
    retry_count: int = 2
    idempotent: bool = True
    retry_safety: bool = True  # prevents duplicate execution on retry
    timeout_minutes: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


# ---------------------------------------------------------------------------
# Scalability Rules
# ---------------------------------------------------------------------------


class ScalabilityRules(BaseModel):
    forecast_horizon_days: int = 365
    expected_growth_percent_per_day: float = 0.0
    peak_forecast_gb_per_day: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


# ---------------------------------------------------------------------------
# PipelineContract
# ---------------------------------------------------------------------------


class PipelineContract(BaseModel):
    """The explicit pipeline contract defining expected behavior."""

    contract_id: str
    pipeline_name: str
    environment: str = "production"
    owner: str = ""
    source: Source
    processing: str = "batch"
    language: str = "pyspark"
    target: Target
    sla: SLARules = Field(default_factory=SLARules)
    schedule: ScheduleRules = Field(default_factory=ScheduleRules)
    reliability: ReliabilityRules = Field(default_factory=ReliabilityRules)
    scalability: ScalabilityRules = Field(default_factory=ScalabilityRules)
    expected_daily_volume_gb: float = 0.0
    peak_daily_volume_gb: float = 0.0
    growth_rate_percent: float = 0.0

    def validate_contract(self) -> list[str]:
        """Validate the contract and return list of issues.

        Named ``validate_contract`` (not ``validate``) to avoid clashing
        with :meth:`pydantic.BaseModel.validate`.
        """
        issues: list[str] = []

        if not self.source.path:
            issues.append("Source path is required")

        if self.expected_daily_volume_gb <= 0:
            issues.append("Expected daily volume must be positive")

        if (
            self.peak_daily_volume_gb > 0
            and self.expected_daily_volume_gb > 0
            and self.peak_daily_volume_gb < self.expected_daily_volume_gb
        ):
            issues.append("Peak volume should be >= expected daily volume")

        if self.sla.max_runtime_minutes <= 0:
            issues.append("SLA max runtime must be positive")

        if self.schedule.frequency not in ("daily", "weekly", "monthly", "hourly", "none"):
            issues.append(f"Unsupported schedule frequency: {self.schedule.frequency}")

        if self.expected_daily_volume_gb > 0 and self.peak_daily_volume_gb == 0:
            issues.append("Peak daily volume should be configured")

        return issues

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "pipeline_name": self.pipeline_name,
            "environment": self.environment,
            "owner": self.owner,
            "source": self.source.to_dict(),
            "processing": self.processing,
            "language": self.language,
            "target": self.target.to_dict(),
            "sla": self.sla.to_dict(),
            "schedule": self.schedule.to_dict(),
            "reliability": self.reliability.to_dict(),
            "scalability": self.scalability.to_dict(),
            "expected_daily_volume_gb": self.expected_daily_volume_gb,
            "peak_daily_volume_gb": self.peak_daily_volume_gb,
            "growth_rate_percent": self.growth_rate_percent,
        }


# ---------------------------------------------------------------------------
# ValidationRun / Score
# ---------------------------------------------------------------------------


class Score(BaseModel):
    """Category-level and overall scores."""

    overall: float = 0.0  # 0-100
    categories: dict[str, float] = Field(default_factory=dict)
    status: str = "UNKNOWN"

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class ValidationRun(BaseModel):
    """A single offline (or live) validation execution."""

    run_id: str
    pipeline_name: str
    contract_id: str = ""
    mode: str = "offline"
    status: str = "UNKNOWN"
    overall_score: float = 0.0
    checkpoints: list[Checkpoint] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    assumptions: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "contract_id": self.contract_id,
            "mode": self.mode,
            "status": self.status,
            "overall_score": self.overall_score,
            "checkpoints": [c.to_dict() for c in self.checkpoints],
            "findings": [f.to_dict() for f in self.findings],
            "timestamp": self.timestamp.isoformat(),
            "assumptions": self.assumptions,
        }
