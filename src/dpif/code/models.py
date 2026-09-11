"""Structured code-analysis models (Phase 4).

All locations are AST-derived (file/line/column). Secret *values* are never
stored — only the kind and location.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from dpif.sql.models import SQLAnalysis


class OperationType(StrEnum):
    READ = "READ"
    WRITE = "WRITE"
    SELECT = "SELECT"
    FILTER = "FILTER"
    JOIN = "JOIN"
    GROUP_BY = "GROUP_BY"
    AGGREGATE = "AGGREGATE"
    DISTINCT = "DISTINCT"
    DROP_DUPLICATES = "DROP_DUPLICATES"
    ORDER_BY = "ORDER_BY"
    WINDOW = "WINDOW"
    REPARTITION = "REPARTITION"
    COALESCE = "COALESCE"
    CACHE = "CACHE"
    PERSIST = "PERSIST"
    COLLECT = "COLLECT"
    TO_PANDAS = "TO_PANDAS"
    SHOW = "SHOW"
    TAKE = "TAKE"
    FIRST = "FIRST"
    COUNT = "COUNT"
    LIMIT = "LIMIT"
    UDF = "UDF"
    PANDAS_UDF = "PANDAS_UDF"
    BROADCAST = "BROADCAST"
    EXPLODE = "EXPLODE"
    UNION = "UNION"
    LOOP = "LOOP"
    CHECKPOINT = "CHECKPOINT"
    OTHER = "OTHER"


class Operation(BaseModel):
    """One detected operation with source location and call context."""

    operation_type: OperationType
    line: int
    column: int = 0
    dataframe: str | None = None
    code: str = ""
    arguments: dict[str, object] = Field(default_factory=dict)
    context: dict[str, object] = Field(default_factory=dict)

    def location(self, filename: str = "") -> str:
        prefix = f"{filename}:" if filename else ""
        return f"{prefix}{self.line}:{self.column}" if self.column else f"{prefix}{self.line}"

    def to_dict(self) -> dict[str, object]:
        return self.model_dump()


class FunctionInfo(BaseModel):
    """A function definition with size/complexity summaries."""

    name: str
    line: int
    end_line: int
    arg_count: int = 0
    branch_count: int = 0
    max_nesting: int = 0

    @property
    def size_lines(self) -> int:
        return max(1, self.end_line - self.line + 1)


class VarAssignment(BaseModel):
    """``target = <expr rooted at source>`` edge for flow analysis."""

    target: str
    source: str = ""
    line: int = 0


class SecretRef(BaseModel):
    """A secret-like string literal (kind + location only, never the value)."""

    line: int
    column: int = 0
    kind: str = "secret"


class PathRef(BaseModel):
    """A hard-coded local path literal with its location."""

    line: int
    column: int = 0
    value: str = ""


class CodeAnalysis(BaseModel):
    """Structured representation of one Python source file."""

    source_file: str = "<code>"
    language: str = "python"
    functions: list[FunctionInfo] = Field(default_factory=list)
    imports: list[str] = Field(default_factory=list)
    dataframe_variables: list[str] = Field(default_factory=list)
    assignments: list[VarAssignment] = Field(default_factory=list)
    operations: list[Operation] = Field(default_factory=list)
    driver_loops: list[dict[str, object]] = Field(default_factory=list)
    secrets: list[SecretRef] = Field(default_factory=list)
    hardcoded_paths: list[PathRef] = Field(default_factory=list)
    lines_of_code: int = 0
    has_exception_handling: bool = False
    max_nesting_depth: int = 0
    parse_error: str | None = None
    sql_analysis: SQLAnalysis | None = None

    def of_type(self, *types: OperationType) -> list[Operation]:
        wanted = set(types)
        return [op for op in self.operations if op.operation_type in wanted]

    def operations_on(self, dataframe: str) -> list[Operation]:
        return [op for op in self.operations if op.dataframe == dataframe]

    def operation_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for op in self.operations:
            counts[op.operation_type.value] = counts.get(op.operation_type.value, 0) + 1
        return counts

    @property
    def function_count(self) -> int:
        return len(self.functions)

    @property
    def average_function_size(self) -> float:
        if not self.functions:
            return 0.0
        return sum(f.size_lines for f in self.functions) / len(self.functions)

    @property
    def maximum_function_size(self) -> int:
        return max((f.size_lines for f in self.functions), default=0)

    def to_dict(self) -> dict[str, object]:
        return self.model_dump()


class AnalysisContext(BaseModel):
    """Joint code + data + source + contract context for contextual rules."""

    pipeline_name: str = "unknown"
    source_type: str | None = None
    source_format: str | None = None
    expected_volume_gb: float | None = None
    peak_volume_gb: float | None = None
    processing_type: str | None = None
    cluster_workers: int | None = None
    contract_name: str | None = None
    evidence_source: str = "profile metadata"
    collection_method: str = "unknown"
    code: CodeAnalysis | None = None
    sql: SQLAnalysis | None = None
    cluster: Any | None = None
    job: Any | None = None
    pipeline_config: Any | None = None

    def to_rule_context(self) -> dict[str, object]:
        """Flatten to the dict the rule/checkpoint engines consume."""
        ctx: dict[str, object] = {
            "source_type": self.source_type,
            "source_format": self.source_format,
            "workload_type": self.processing_type,
            "evidence_source": self.evidence_source,
            "collection_method": self.collection_method,
        }
        if self.expected_volume_gb is not None:
            ctx["data_size_gb"] = self.expected_volume_gb
            ctx["expected_volume_gb"] = self.expected_volume_gb
        if self.peak_volume_gb is not None:
            ctx["peak_volume_gb"] = self.peak_volume_gb
        if self.cluster_workers is not None:
            ctx["cluster_workers"] = self.cluster_workers
        if self.code and self.code.sql_analysis:
            ctx["sql_analysis"] = self.code.sql_analysis
        elif self.sql:
            ctx["sql_analysis"] = self.sql
        if self.cluster is not None:
            ctx["cluster"] = self.cluster
        if self.job is not None:
            ctx["job"] = self.job
        if self.pipeline_config is not None:
            ctx["pipeline_config"] = self.pipeline_config
        return {k: v for k, v in ctx.items() if v is not None}
