"""SQL Parser using sqlglot (Phase 5).

Parses SQL into structured models. Supports Databricks/Spark SQL dialect.
"""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel
from sqlglot.errors import ParseError as SqlglotParseError

from dpif.sql.models import (
    SQLCTE,
    JoinType,
    QueryType,
    SQLAggregation,
    SQLAnalysis,
    SQLDialect,
    SQLFilter,
    SQLJoin,
    SQLOrderBy,
    SQLParseError,
    SQLParseResult,
    SQLProjection,
    SQLQuery,
    SQLSubquery,
    SQLTableReference,
    SQLUnion,
    SQLWindow,
)


class SQLParser:
    """Parse SQL into structured models using sqlglot."""

    DIALECT_MAP = {
        SQLDialect.DATABRICKS: "databricks",
        SQLDialect.SPARK: "spark",
        SQLDialect.HIVE: "hive",
        SQLDialect.PRESTO: "presto",
        SQLDialect.GENERIC: "",
    }

    def __init__(self, dialect: SQLDialect = SQLDialect.DATABRICKS):
        self.dialect = dialect
        self.dialect_str = self.DIALECT_MAP.get(dialect, "spark")

    def parse(self, sql: str, source_file: str = "<sql>") -> SQLParseResult:
        """Parse SQL string into structured analysis."""
        errors: list[SQLParseError] = []
        queries: list[SQLQuery] = []

        if not sql or not sql.strip():
            analysis = SQLAnalysis(source_file=source_file, dialect=self.dialect)
            return SQLParseResult(analysis=analysis, original_sql=sql, parse_errors=[])

        try:
            # sqlglot.parse parses multi-statement scripts
            parsed_trees = sqlglot.parse(sql, read=self.dialect_str, error_level=ErrorLevel.RAISE)
        except SqlglotParseError as e:
            if hasattr(e, "errors") and e.errors:
                for err_dict in e.errors:
                    errors.append(
                        SQLParseError(
                            file=source_file,
                            line=int(err_dict.get("line", 1) or 1),
                            column=int(err_dict.get("col", 1) or 1),
                            message=str(err_dict.get("description", str(e))),
                            severity="ERROR",
                        )
                    )
            else:
                errors.append(
                    SQLParseError(
                        file=source_file,
                        line=getattr(e, "line", 1) or 1,
                        column=getattr(e, "col", 1) or 1,
                        message=str(e),
                        severity="ERROR",
                    )
                )
            analysis = SQLAnalysis(
                source_file=source_file,
                dialect=self.dialect,
                parse_errors=errors,
            )
            return SQLParseResult(analysis=analysis, original_sql=sql, parse_errors=errors)
        except Exception as e:
            errors.append(
                SQLParseError(
                    file=source_file,
                    line=1,
                    column=1,
                    message=f"SQL parse error: {e}",
                    severity="ERROR",
                )
            )
            analysis = SQLAnalysis(
                source_file=source_file,
                dialect=self.dialect,
                parse_errors=errors,
            )
            return SQLParseResult(analysis=analysis, original_sql=sql, parse_errors=errors)

        for tree in parsed_trees:
            if tree is None:
                continue
            try:
                q = self._convert_ast(tree, source_file)
                if q:
                    queries.append(q)
            except Exception as e:
                errors.append(
                    SQLParseError(
                        file=source_file,
                        line=getattr(tree, "line", 1) or 1,
                        column=getattr(tree, "col", 1) or 1,
                        message=f"Error analyzing statement: {e}",
                        severity="ERROR",
                    )
                )

        all_tables: list[SQLTableReference] = []
        all_joins: list[SQLJoin] = []
        all_filters: list[SQLFilter] = []
        all_aggs: list[SQLAggregation] = []
        all_windows: list[SQLWindow] = []
        all_ctes: list[SQLCTE] = []
        all_subqueries: list[SQLSubquery] = []
        all_unions: list[SQLUnion] = []
        wildcard_projections: list[SQLProjection] = []

        total_complexity: dict[str, int] = {
            "join_count": 0,
            "cte_count": 0,
            "subquery_count": 0,
            "aggregation_count": 0,
            "window_count": 0,
            "union_count": 0,
            "case_count": 0,
            "max_nesting": 0,
        }

        for q in queries:
            all_tables.extend(q.tables)
            all_joins.extend(q.joins)
            all_filters.extend(q.filters)
            all_aggs.extend(q.aggregations)
            all_windows.extend(q.windows)
            all_ctes.extend(q.ctes)
            all_subqueries.extend(q.subqueries)
            all_unions.extend(q.unions)
            if q.projection and q.projection.has_wildcard:
                wildcard_projections.append(q.projection)

            for k in total_complexity:
                if k == "max_nesting":
                    total_complexity[k] = max(
                        total_complexity[k],
                        q.complexity.get("max_nesting", 0),
                    )
                else:
                    total_complexity[k] += q.complexity.get(k, 0)

        analysis = SQLAnalysis(
            source_file=source_file,
            dialect=self.dialect,
            query_count=len(queries),
            queries=queries,
            tables=all_tables,
            joins=all_joins,
            filters=all_filters,
            aggregations=all_aggs,
            windows=all_windows,
            ctes=all_ctes,
            subqueries=all_subqueries,
            unions=all_unions,
            wildcard_projections=wildcard_projections,
            complexity=total_complexity,
            parse_errors=errors,
        )

        return SQLParseResult(
            analysis=analysis,
            original_sql=sql,
            parse_errors=errors,
        )

    def _convert_ast(self, node: Any, source_file: str) -> SQLQuery:
        """Convert sqlglot AST to our SQLQuery model."""
        sql_str = node.sql(dialect=self.dialect_str)
        line = getattr(node, "line", 1) or 1
        col = getattr(node, "col", 1) or 1

        query_type = QueryType.UNKNOWN
        if isinstance(node, exp.Select):
            query_type = QueryType.SELECT
        elif isinstance(node, exp.Insert):
            query_type = QueryType.INSERT
        elif isinstance(node, exp.Update):
            query_type = QueryType.UPDATE
        elif isinstance(node, exp.Delete):
            query_type = QueryType.DELETE
        elif isinstance(node, exp.Create):
            query_type = QueryType.CREATE
        elif isinstance(node, exp.Drop):
            query_type = QueryType.DROP
        elif isinstance(node, exp.Alter):
            query_type = QueryType.ALTER

        tables = self._extract_tables(node)
        joins = self._extract_joins(node)
        filters = self._extract_filters(node)
        aggregations = self._extract_aggregations(node)
        group_by = self._extract_group_by(node)
        having = self._extract_having(node)
        order_by = self._extract_order_by(node)
        windows = self._extract_windows(node)
        ctes = self._extract_ctes(node)
        subqueries = self._extract_subqueries(node)
        unions = self._extract_unions(node)
        projection = self._extract_projection(node) if isinstance(node, exp.Select) else None

        distinct = False
        limit_val: int | None = None
        if isinstance(node, exp.Select):
            distinct = bool(node.args.get("distinct"))
            limit_exp = node.args.get("limit")
            if limit_exp and hasattr(limit_exp, "expression"):
                try:
                    limit_val = int(str(limit_exp.expression))
                except (ValueError, TypeError):
                    limit_val = None

        complexity = self._calculate_complexity(node)

        return SQLQuery(
            query_type=query_type,
            sql=sql_str,
            normalized_sql=sql_str,
            tables=tables,
            joins=joins,
            filters=filters,
            aggregations=aggregations,
            order_by=order_by,
            windows=windows,
            ctes=ctes,
            subqueries=subqueries,
            unions=unions,
            projection=projection,
            group_by=group_by,
            having=having,
            distinct=distinct,
            limit=limit_val,
            complexity=complexity,
            line=line,
            column=col,
        )

    def _extract_projection(self, node: exp.Select) -> SQLProjection:
        """Extract SELECT projection including wildcard star detection."""
        expressions: list[str] = []
        has_wildcard = False
        wildcard_tables: list[str] = []

        for expr in node.expressions:
            expr_str = expr.sql(dialect=self.dialect_str)
            expressions.append(expr_str)
            if isinstance(expr, exp.Star):
                has_wildcard = True
            elif isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                has_wildcard = True
                if expr.table:
                    wildcard_tables.append(expr.table)
            elif expr_str.strip() == "*" or expr_str.endswith(".*"):
                has_wildcard = True
                if "." in expr_str:
                    table_part = expr_str.rsplit(".*", 1)[0].strip()
                    if table_part:
                        wildcard_tables.append(table_part)

        return SQLProjection(
            expressions=expressions,
            has_wildcard=has_wildcard,
            wildcard_tables=wildcard_tables,
            line=getattr(node, "line", 1) or 1,
            column=getattr(node, "col", 1) or 1,
        )

    def _extract_tables(self, node: exp.Expression) -> list[SQLTableReference]:
        """Extract table references from query."""
        tables: list[SQLTableReference] = []
        seen = set()

        for tbl in node.find_all(exp.Table):
            name = tbl.name
            alias = tbl.alias or None
            db = tbl.args.get("db")
            schema = getattr(db, "name", None) or (str(db) if db else None)
            cat_node = tbl.args.get("catalog")
            catalog = getattr(cat_node, "name", None)

            key = (catalog, schema, name, alias)
            if key not in seen:
                seen.add(key)
                tables.append(
                    SQLTableReference(
                        name=name,
                        alias=alias,
                        schema=schema,
                        catalog=catalog,
                        line=getattr(tbl, "line", 0) or 0,
                        column=getattr(tbl, "col", 0) or 0,
                    )
                )

        return tables

    def _extract_joins(self, node: exp.Expression) -> list[SQLJoin]:
        """Extract JOIN operations and distinguish cross joins."""
        joins: list[SQLJoin] = []

        for select in node.find_all(exp.Select):
            from_clause = select.args.get("from")
            left_table = None
            if from_clause and isinstance(from_clause.this, exp.Table):
                left_table = self._table_to_ref(from_clause.this)

            for join in select.args.get("joins", []):
                join_kind = (join.kind or "").upper().strip()
                join_side = (join.side or "").upper().strip()

                if "CROSS" in join_kind or "CROSS" in join_side:
                    join_type = JoinType.CROSS
                elif "LEFT" in join_side:
                    join_type = JoinType.LEFT
                elif "RIGHT" in join_side:
                    join_type = JoinType.RIGHT
                elif "FULL" in join_side:
                    join_type = JoinType.FULL
                elif join_kind == "INNER":
                    join_type = JoinType.INNER
                elif not join.args.get("on") and not join.args.get("using"):
                    # Join without ON or USING condition is Cartesian / Cross Join
                    join_type = JoinType.CROSS
                else:
                    join_type = JoinType.INNER

                right_table = None
                if isinstance(join.this, exp.Table):
                    right_table = self._table_to_ref(join.this)

                condition = ""
                join_cols: list[str] = []
                on_expr = join.args.get("on")
                if on_expr is not None:
                    condition = on_expr.sql(dialect=self.dialect_str)
                    join_cols = [c.name for c in on_expr.find_all(exp.Column)]
                elif join.args.get("using"):
                    using_cols = join.args["using"]
                    join_cols = [c.name for c in using_cols if hasattr(c, "name")]
                    condition = f"USING ({', '.join(join_cols)})"

                joins.append(
                    SQLJoin(
                        join_type=join_type,
                        left_table=left_table or SQLTableReference(name=""),
                        right_table=right_table or SQLTableReference(name=""),
                        condition=condition,
                        join_columns=join_cols,
                        line=getattr(join, "line", 0) or 0,
                        column=getattr(join, "col", 0) or 0,
                    )
                )

        return joins

    def _table_to_ref(self, table: exp.Table) -> SQLTableReference:
        db = table.args.get("db")
        schema = getattr(db, "name", None) or (str(db) if db else None)
        cat_node = table.args.get("catalog")
        catalog = getattr(cat_node, "name", None)
        return SQLTableReference(
            name=table.name,
            alias=table.alias or None,
            schema=schema,
            catalog=catalog,
            line=getattr(table, "line", 0) or 0,
            column=getattr(table, "col", 0) or 0,
        )

    def _extract_filters(self, node: exp.Expression) -> list[SQLFilter]:
        """Extract WHERE and HAVING filter predicates."""
        filters: list[SQLFilter] = []

        for select in node.find_all(exp.Select):
            where = select.args.get("where")
            if where and where.this is not None:
                filters.append(
                    SQLFilter(
                        expression=where.this.sql(dialect=self.dialect_str),
                        columns=self._extract_columns(where.this),
                        functions_on_columns=self._extract_functions_on_columns(where.this),
                        line=getattr(where, "line", 0) or 0,
                        column=getattr(where, "col", 0) or 0,
                    )
                )
            having = select.args.get("having")
            if having and having.this is not None:
                filters.append(
                    SQLFilter(
                        expression=having.this.sql(dialect=self.dialect_str),
                        columns=self._extract_columns(having.this),
                        functions_on_columns=self._extract_functions_on_columns(having.this),
                        line=getattr(having, "line", 0) or 0,
                        column=getattr(having, "col", 0) or 0,
                    )
                )

        return filters

    def _extract_columns(self, expr: exp.Expression) -> list[str]:
        return sorted({c.name for c in expr.find_all(exp.Column)})

    def _extract_functions_on_columns(self, expr: exp.Expression) -> list[str]:
        """Detect scalar functions applied directly to filter columns in predicates."""
        funcs: list[str] = []
        for n in expr.walk():
            # Match function calls or casts that wrap column references
            if isinstance(n, (exp.Func, exp.Cast, exp.TryCast, exp.Anonymous)) and not isinstance(
                n, (exp.Connector, exp.Binary, exp.AggFunc)
            ):
                col_children = list(n.find_all(exp.Column))
                if col_children:
                    fname = getattr(n, "key", "") or type(n).__name__
                    funcs.append(fname.upper())
        return sorted(set(funcs))

    def _extract_aggregations(self, node: exp.Expression) -> list[SQLAggregation]:
        aggs: list[SQLAggregation] = []
        for agg in node.find_all(exp.AggFunc):
            col_name = None
            col_child = next(agg.find_all(exp.Column), None)
            if col_child is not None:
                col_name = col_child.name
            is_distinct = (
                bool(agg.args.get("distinct"))
                or isinstance(agg.this, exp.Distinct)
                or any(isinstance(a, exp.Distinct) for a in agg.args.values())
            )
            aggs.append(
                SQLAggregation(
                    function=type(agg).__name__.upper(),
                    column=col_name,
                    distinct=is_distinct,
                    line=getattr(agg, "line", 0) or 0,
                    col_offset=getattr(agg, "col", 0) or 0,
                )
            )
        return aggs

    def _extract_group_by(self, node: exp.Expression) -> list[str]:
        group_by: list[str] = []
        for select in node.find_all(exp.Select):
            group = select.args.get("group")
            if group is not None:
                for expr in getattr(group, "expressions", []):
                    group_by.append(expr.sql(dialect=self.dialect_str))
        return group_by

    def _extract_having(self, node: exp.Expression) -> list[SQLFilter]:
        filters: list[SQLFilter] = []
        for select in node.find_all(exp.Select):
            having = select.args.get("having")
            if having and having.this is not None:
                filters.append(
                    SQLFilter(
                        expression=having.this.sql(dialect=self.dialect_str),
                        columns=self._extract_columns(having.this),
                        functions_on_columns=self._extract_functions_on_columns(having.this),
                        line=getattr(having, "line", 0) or 0,
                        column=getattr(having, "col", 0) or 0,
                    )
                )
        return filters

    def _extract_order_by(self, node: exp.Expression) -> list[SQLOrderBy]:
        orders: list[SQLOrderBy] = []
        for order in node.find_all(exp.Order):
            for e in getattr(order, "expressions", []):
                col_name = (
                    e.this.name
                    if isinstance(e.this, exp.Column)
                    else e.this.sql(dialect=self.dialect_str)
                )
                direction = "DESC" if e.args.get("desc") else "ASC"
                orders.append(
                    SQLOrderBy(
                        column=col_name,
                        direction=direction,
                        line=getattr(e, "line", 0) or 0,
                        col_offset=getattr(e, "col", 0) or 0,
                    )
                )
        return orders

    def _extract_windows(self, node: exp.Expression) -> list[SQLWindow]:
        windows: list[SQLWindow] = []
        for win in node.find_all(exp.Window):
            partition_by: list[str] = []
            order_by: list[str] = []
            frame_str = ""
            bounded = False

            for p in win.args.get("partition_by") or []:
                partition_by.append(
                    p.name if isinstance(p, exp.Column) else p.sql(dialect=self.dialect_str)
                )
            order_spec = win.args.get("order")
            if order_spec is not None:
                for o in getattr(order_spec, "expressions", []):
                    order_by.append(o.sql(dialect=self.dialect_str))

            spec = win.args.get("spec")
            if spec is not None:
                frame_str = spec.sql(dialect=self.dialect_str)
                start = str(spec.args.get("start") or "").upper()
                end = str(spec.args.get("end") or "").upper()
                if "UNBOUNDED" in start and "UNBOUNDED" in end:
                    bounded = False
                else:
                    bounded = True

            fn_name = ""
            if win.this is not None:
                fn_name = type(win.this).__name__.upper()

            windows.append(
                SQLWindow(
                    function=fn_name,
                    partition_by=partition_by,
                    order_by=order_by,
                    frame=frame_str,
                    bounded=bounded,
                    line=getattr(win, "line", 0) or 0,
                    column=getattr(win, "col", 0) or 0,
                )
            )
        return windows

    def _extract_ctes(self, node: exp.Expression) -> list[SQLCTE]:
        ctes: list[SQLCTE] = []
        for cte in node.find_all(exp.CTE):
            ctes.append(
                SQLCTE(
                    name=cte.alias or "",
                    definition=cte.this.sql(dialect=self.dialect_str),
                    line=getattr(cte, "line", 0) or 0,
                    column=getattr(cte, "col", 0) or 0,
                )
            )
        return ctes

    def _extract_subqueries(self, node: exp.Expression) -> list[SQLSubquery]:
        subqueries: list[SQLSubquery] = []
        for sub in node.find_all(exp.Subquery):
            subqueries.append(
                SQLSubquery(
                    type="DERIVED",
                    sql=sub.sql(dialect=self.dialect_str),
                    line=getattr(sub, "line", 0) or 0,
                    column=getattr(sub, "col", 0) or 0,
                )
            )
        return subqueries

    def _extract_unions(self, node: exp.Expression) -> list[SQLUnion]:
        unions: list[SQLUnion] = []
        for u in node.find_all(exp.Union):
            union_type = "UNION ALL" if u.args.get("distinct") is False else "UNION"
            unions.append(
                SQLUnion(
                    type=union_type,
                    left_query=u.left.sql(dialect=self.dialect_str) if u.left else "",
                    right_query=u.right.sql(dialect=self.dialect_str) if u.right else "",
                    line=getattr(u, "line", 0) or 0,
                    column=getattr(u, "col", 0) or 0,
                )
            )
        return unions

    def _calculate_complexity(self, node: exp.Expression) -> dict[str, int]:
        max_nesting = 0
        for item in node.walk():
            depth = 0
            curr = item.parent
            while curr is not None:
                if isinstance(curr, (exp.Select, exp.Subquery)):
                    depth += 1
                curr = curr.parent
            max_nesting = max(max_nesting, depth)

        return {
            "join_count": len(list(node.find_all(exp.Join))),
            "cte_count": len(list(node.find_all(exp.CTE))),
            "subquery_count": len(list(node.find_all(exp.Subquery))),
            "aggregation_count": len(list(node.find_all(exp.AggFunc))),
            "window_count": len(list(node.find_all(exp.Window))),
            "union_count": len(list(node.find_all(exp.Union))),
            "case_count": len(list(node.find_all(exp.Case))),
            "max_nesting": max_nesting,
        }


def parse_sql(
    sql: str,
    source_file: str = "<sql>",
    dialect: SQLDialect = SQLDialect.DATABRICKS,
) -> SQLParseResult:
    """Convenience function to parse SQL."""
    parser = SQLParser(dialect)
    return parser.parse(sql, source_file)
