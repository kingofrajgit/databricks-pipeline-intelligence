# ADR-006: SQL Intelligence with sqlglot (Phase 5)

## Status: Accepted (Phase 5)

## Context
Phase 4 introduced AST-based static intelligence for Python and PySpark pipelines. However, Databricks pipelines frequently express critical transformations in SQL—either as standalone `.sql` scripts or embedded inside Python via `spark.sql(...)` literals and variables. Assessing pipeline readiness requires deep structural analysis of SQL without relying on brittle regex matching or executing SQL against live Databricks compute.

## Decision

### 1. sqlglot AST Parsing targeting Databricks / Spark SQL Dialect
- Adopt `sqlglot>=25.0` as the SQL parsing foundation.
- Target Databricks / Spark SQL dialect (`read="spark"` / `"databricks"`).
- Reject regular expressions for structural SQL inspection. Regex is strictly forbidden for query grammar analysis.
- Handle syntax errors gracefully via sqlglot's `ErrorLevel.RAISE` to extract exact line and column locations into `SQLParseError` objects without aborting pipeline analysis.

### 2. Standalone and Embedded SQL Extraction
- Standalone `.sql` files are parsed directly into `SQLAnalysis`.
- In Python files, the Python AST visitor locates `spark.sql(...)` invocations, resolving string literals as well as local string variables (`sql_query = "..."`).
- Embedded SQL analyses are merged into the parent `CodeAnalysis.sql_analysis`, ensuring unified representation across mixed Python/SQL pipelines.

### 3. Structured SQL Models
Extract structured queries into `SQLAnalysis`:
- Projections: tracking wildcard `SELECT *` vs explicit column lists.
- Table References: tracking catalogs, schemas, table names, and aliases.
- Joins: classifying join types (CROSS, INNER, LEFT, FULL, COMMA) and extracting join conditions/keys.
- Filters: detecting predicates and scalar functions applied to columns (e.g. `UPPER(col)`, `DATE_TRUNC(...)`, `CAST(...)`).
- Aggregations: detecting GROUP BY, HAVING, and DISTINCT aggregates (`COUNT(DISTINCT)`).
- Windows: identifying OVER clauses, partitioning, ordering, and window frames (`ROWS/RANGE BETWEEN`).
- Complexity Metrics: counting CTEs, subqueries, joins, projections, and AST nesting depth.

### 4. Integration into CP-004 (Code Validation Checkpoint)
- SQL intelligence does NOT create a separate checkpoint or standalone finding type; it integrates directly into `CP-004` (Code Validation Checkpoint) and DPIF readiness scoring.
- Checkpoint engine passes `sql_analysis` into rule evaluation data.
- Rules starting with `CODE-SQL-` route to `dpif.sql.evaluators`.

### 5. Context-Aware Evaluators (CODE-SQL-001 through 010)
- 10 externalized YAML rule definitions (`rules/code/CODE-SQL-*.yaml`):
  - `CODE-SQL-001`: Wildcard Projection (`SELECT *`, `table.*`)
  - `CODE-SQL-002`: Cross Join Risk (explicit `CROSS JOIN` and comma joins)
  - `CODE-SQL-003`: Potential Large Join (contextual volume evaluation)
  - `CODE-SQL-004`: Large Aggregation / High Cardinality Group By
  - `CODE-SQL-005`: Global ORDER BY Without LIMIT on Large Inputs
  - `CODE-SQL-006`: Expensive DISTINCT / COUNT(DISTINCT)
  - `CODE-SQL-007`: Unbounded Window Frame Specification
  - `CODE-SQL-008`: Function on Filter Column (predicate pushdown impairment)
  - `CODE-SQL-009`: Excessive Query Complexity (joins, CTEs, subqueries)
  - `CODE-SQL-010`: SQL Parse Error (blocking syntax error)
- Strictly externalized rule thresholds (no hardcoded limits in code).

### 6. Strict Preservation of Core DPIF Principles
- **UNKNOWN Semantics**: Missing data size or absent schema yields `UNKNOWN` (confidence 0.5), never silent `PASS` or speculative `FAIL`.
- **Authoritative Provenance**: Location lines, columns, and source files are preserved.
- **Backward Compatibility**: All Phase 1–4 checkpoints (`CP-001` through `CP-003`), scoring algorithms, and existing tests remain 100% green.

## Consequences
- 10 SQL rules (CODE-SQL-001…010) backed by pure detector functions and bridge evaluators.
- 56 comprehensive unit & integration tests covering positive, negative, edge cases, false positives, volume scaling matrix, threshold overrides, embedded SQL, and checkpoint integration.
- 303 total automated tests pass green across the entire repository.
- Full type-safety verified via mypy; strict code style verified via ruff.

## Related
- ADR-002: Checkpoint dependency propagation
- ADR-004: Source/Data Intelligence (Phase 3)
- ADR-005: AST-Based Code Intelligence (Phase 4)
