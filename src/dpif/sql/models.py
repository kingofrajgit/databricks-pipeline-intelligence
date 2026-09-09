"""SQL Analysis models (Phase 5)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class SQLDialect(StrEnum):
    """Supported SQL dialects."""

    DATABRICKS = "databricks"
    SPARK = "spark"
    HIVE = "hive"
    PRESTO = "presto"
    GENERIC = "generic"


class JoinType(StrEnum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    FULL_OUTER = "FULL OUTER"
    LEFT_SEMI = "LEFT SEMI"
    LEFT_ANTI = "LEFT ANTI"
    CROSS = "CROSS"
    UNKNOWN = "UNKNOWN"


class QueryType(StrEnum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    WITH = "WITH"
    UNKNOWN = "UNKNOWN"


class SQLParseError(BaseModel):
    """A parse error from the SQL parser."""

    file: str
    line: int
    column: int
    message: str
    severity: str = "ERROR"  # ERROR, WARNING


class SQLTableReference(BaseModel):
    """A table reference in a query."""

    model_config = {"protected_namespaces": ()}

    name: str
    alias: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    catalog: str | None = None
    is_subquery: bool = False
    subquery_alias: str | None = None
    line: int = 0
    column: int = 0

    @property
    def schema(self) -> str | None:  # type: ignore[override]
        return self.schema_name


class SQLJoin(BaseModel):
    """A JOIN operation."""

    join_type: JoinType = JoinType.UNKNOWN
    left_table: SQLTableReference
    right_table: SQLTableReference
    condition: str = ""
    join_columns: list[str] = Field(default_factory=list)
    line: int = 0
    column: int = 0


class SQLFilter(BaseModel):
    """A WHERE/HAVING filter."""

    expression: str
    columns: list[str] = Field(default_factory=list)
    functions_on_columns: list[str] = Field(default_factory=list)
    line: int = 0
    column: int = 0


class SQLAggregation(BaseModel):
    """An aggregation operation."""

    function: str  # SUM, COUNT, AVG, MIN, MAX, COUNT_DISTINCT, etc.
    column: str | None = None
    distinct: bool = False
    line: int = 0
    col_offset: int = 0


class SQLOrderBy(BaseModel):
    """An ORDER BY clause."""

    column: str
    direction: str = "ASC"  # ASC, DESC
    line: int = 0
    col_offset: int = 0


class SQLWindow(BaseModel):
    """A window function specification."""

    function: str  # ROW_NUMBER, RANK, LAG, LEAD, etc.
    partition_by: list[str] = Field(default_factory=list)
    order_by: list[str] = Field(default_factory=list)
    frame: str = ""  # ROWS BETWEEN ..., RANGE BETWEEN ...
    bounded: bool = False
    line: int = 0
    column: int = 0


class SQLCTE(BaseModel):
    """A Common Table Expression."""

    name: str
    definition: str  # the SQL defining the CTE
    referenced_by: list[str] = Field(default_factory=list)  # queries that reference this CTE
    line: int = 0
    column: int = 0


class SQLSubquery(BaseModel):
    """A subquery."""

    type: str  # SCALAR, IN, EXISTS, DERIVED
    sql: str
    parent_query: str | None = None
    line: int = 0
    column: int = 0


class SQLUnion(BaseModel):
    """A UNION/UNION ALL operation."""

    type: str  # UNION, UNION ALL
    left_query: str
    right_query: str
    line: int = 0
    column: int = 0


class SQLProjection(BaseModel):
    """A projection (SELECT list)."""

    expressions: list[str] = Field(default_factory=list)
    has_wildcard: bool = False
    wildcard_tables: list[str] = Field(default_factory=list)  # tables with *
    line: int = 0
    column: int = 0


class SQLQuery(BaseModel):
    """A parsed SQL query."""

    query_type: QueryType = QueryType.UNKNOWN
    sql: str
    normalized_sql: str = ""
    tables: list[SQLTableReference] = Field(default_factory=list)
    joins: list[SQLJoin] = Field(default_factory=list)
    filters: list[SQLFilter] = Field(default_factory=list)
    aggregations: list[SQLAggregation] = Field(default_factory=list)
    order_by: list[SQLOrderBy] = Field(default_factory=list)
    windows: list[SQLWindow] = Field(default_factory=list)
    ctes: list[SQLCTE] = Field(default_factory=list)
    subqueries: list[SQLSubquery] = Field(default_factory=list)
    unions: list[SQLUnion] = Field(default_factory=list)
    projection: SQLProjection | None = None
    group_by: list[str] = Field(default_factory=list)
    having: list[SQLFilter] = Field(default_factory=list)
    distinct: bool = False
    limit: int | None = None
    complexity: dict[str, Any] = Field(default_factory=dict)
    line: int = 0
    column: int = 0
    parse_errors: list[SQLParseError] = Field(default_factory=list)


class SQLAnalysis(BaseModel):
    """Complete SQL analysis result."""

    source_file: str = "<sql>"
    dialect: SQLDialect = SQLDialect.DATABRICKS
    query_count: int = 0
    queries: list[SQLQuery] = Field(default_factory=list)
    tables: list[SQLTableReference] = Field(default_factory=list)
    joins: list[SQLJoin] = Field(default_factory=list)
    filters: list[SQLFilter] = Field(default_factory=list)
    aggregations: list[SQLAggregation] = Field(default_factory=list)
    windows: list[SQLWindow] = Field(default_factory=list)
    ctes: list[SQLCTE] = Field(default_factory=list)
    subqueries: list[SQLSubquery] = Field(default_factory=list)
    unions: list[SQLUnion] = Field(default_factory=list)
    wildcard_projections: list[SQLProjection] = Field(default_factory=list)
    complexity: dict[str, Any] = Field(default_factory=dict)
    parse_errors: list[SQLParseError] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class SQLParseResult(BaseModel):
    """Result of parsing SQL (may contain multiple queries)."""

    analysis: SQLAnalysis
    original_sql: str
    parse_errors: list[SQLParseError] = Field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return len(self.parse_errors) > 0
