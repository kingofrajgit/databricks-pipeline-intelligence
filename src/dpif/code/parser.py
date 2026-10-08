"""Python AST parser producing structured CodeAnalysis (Phase 4).

Regex is used only to *classify* string-literal nodes already located by the
AST (secret-like names, local-path shapes) — never to find code structure.
"""

from __future__ import annotations

import ast
import re
from typing import Any

from dpif.code.models import (
    CodeAnalysis,
    FunctionInfo,
    Operation,
    OperationType,
    PathRef,
    SecretRef,
    VarAssignment,
)

METHOD_OPS: dict[str, OperationType] = {
    "select": OperationType.SELECT,
    "withColumn": OperationType.SELECT,
    "filter": OperationType.FILTER,
    "where": OperationType.FILTER,
    "join": OperationType.JOIN,
    "crossJoin": OperationType.JOIN,
    "groupBy": OperationType.GROUP_BY,
    "groupby": OperationType.GROUP_BY,
    "agg": OperationType.AGGREGATE,
    "distinct": OperationType.DISTINCT,
    "dropDuplicates": OperationType.DROP_DUPLICATES,
    "drop_duplicates": OperationType.DROP_DUPLICATES,
    "orderBy": OperationType.ORDER_BY,
    "order_by": OperationType.ORDER_BY,
    "sort": OperationType.ORDER_BY,
    "repartition": OperationType.REPARTITION,
    "coalesce": OperationType.COALESCE,
    "cache": OperationType.CACHE,
    "persist": OperationType.PERSIST,
    "collect": OperationType.COLLECT,
    "toPandas": OperationType.TO_PANDAS,
    "to_pandas": OperationType.TO_PANDAS,
    "show": OperationType.SHOW,
    "take": OperationType.TAKE,
    "head": OperationType.TAKE,
    "first": OperationType.FIRST,
    "count": OperationType.COUNT,
    "limit": OperationType.LIMIT,
    "union": OperationType.UNION,
    "unionByName": OperationType.UNION,
    "explode": OperationType.EXPLODE,
    "over": OperationType.WINDOW,
    "unpersist": OperationType.PERSIST,
    "checkpoint": OperationType.CHECKPOINT,
    "localCheckpoint": OperationType.CHECKPOINT,
    "localcheckpoint": OperationType.CHECKPOINT,
}

TERMINAL_ACTIONS = frozenset(
    {
        "collect",
        "toPandas",
        "to_pandas",
        "take",
        "head",
        "first",
        "show",
        "count",
    }
)

SPARK_READ_ATTRS = frozenset({"read", "sql", "table", "load"})
SPARK_WRITE_ATTRS = frozenset({"write", "save", "saveAsTable", "insertInto"})
WRITE_CHAIN_ATTRS = frozenset(
    {
        "save",
        "parquet",
        "csv",
        "json",
        "mode",
        "format",
        "partitionBy",
        "option",
        "options",
    }
)

WINDOW_SPEC_ATTRS = frozenset({"partitionBy", "orderBy", "rowsBetween", "rangeBetween"})

SECRET_NAME_RE = re.compile(
    r"(?i)(password|passwd|secret|token|api[_-]?key|storage[_-]?key|sas[_-]?token)"
)
PATH_VALUE_RE = re.compile(r"^(?:/tmp/|/home/|/var/|file:|[A-Za-z]:\\)")

_JOIN_ARG_TYPES = (
    "inner",
    "outer",
    "full",
    "fullouter",
    "full_outer",
    "left",
    "leftouter",
    "left_outer",
    "leftsemi",
    "left_semi",
    "right",
    "rightouter",
    "right_outer",
    "leftanti",
    "cross",
)


def _chain(call: ast.Call) -> tuple[str | None, list[str]]:
    """Resolve ``a.b(...).c(...)`` into (root name, [attrs outer-to-inner]).

    The walk starts at the outermost call, so collected attributes are
    already outermost-first; entry points (``spark.read``, ``df.write``)
    append innermost-last. ``outer = attrs[0]``.
    """
    attrs: list[str] = []
    node: ast.AST = call
    while True:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attrs.append(node.func.attr)
            node = node.func.value
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Call):
            # Bare attribute links inside chains (e.g. ``.write`` in
            # ``df.coalesce(1).write.format(..).save(..)``).
            attrs.append(node.attr)
            node = node.value
        else:
            break
    if isinstance(node, ast.Name):
        return node.id, attrs
    if isinstance(node, ast.Attribute):
        # Entry points such as spark.read / df.write / Window.partitionBy.
        if isinstance(node.value, ast.Name):
            return node.value.id, [*attrs, node.attr]
        return None, attrs
    return None, attrs


def _snippet(source: str, node: ast.AST, limit: int = 160) -> str:
    try:
        text = ast.get_source_segment(source, node) or ""
    except Exception:
        text = ""
    one_line = " ".join(text.split())
    return one_line[:limit]


def _const_str(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _const_number(node: ast.AST) -> float | int | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    return None


class _StructureVisitor(ast.NodeVisitor):
    """Collects functions, imports, df vars, loops, secrets, complexity."""

    def __init__(self, source: str) -> None:
        self.source = source
        self.functions: list[FunctionInfo] = []
        self.imports: list[str] = []
        self.df_vars: list[str] = []
        self.assignments: list[VarAssignment] = []
        self.collected_vars: dict[str, str] = {}  # var -> COLLECT/TO_PANDAS/TAKE
        self.driver_loops: list[dict[str, object]] = []
        self.secrets: list[SecretRef] = []
        self.paths: list[PathRef] = []
        self.string_vars: dict[str, str] = {}
        self.has_try = False
        self.max_nesting = 0
        self._depth = 0
        self._func_stack: list[FunctionInfo] = []

    # -- nesting tracking -------------------------------------------------
    def _nested(self, node: ast.AST) -> None:
        self._depth += 1
        self.max_nesting = max(self.max_nesting, self._depth)
        if self._func_stack:
            top = self._func_stack[-1]
            self._func_stack[-1] = top.model_copy(
                update={"max_nesting": max(top.max_nesting, self._depth)}
            )
        self.generic_visit(node)
        self._depth -= 1

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        info = FunctionInfo(
            name=node.name,
            line=node.lineno,
            end_line=getattr(node, "end_lineno", node.lineno) or node.lineno,
            arg_count=len(node.args.args),
        )
        # decorators: @udf / @pandas_udf (operations recorded in pass B)
        self.functions.append(info)
        self._func_stack.append(info)
        self._nested(node)
        finished = self._func_stack.pop()
        self.functions[self.functions.index(info)] = finished

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_If(self, node: ast.If) -> None:
        self._bump_branch()
        self._nested(node)

    def visit_For(self, node: ast.For) -> None:
        self._bump_branch()
        self._check_driver_loop(node)
        self._nested(node)

    def visit_While(self, node: ast.While) -> None:
        self._bump_branch()
        self._nested(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        self._bump_branch()
        self._nested(node)

    def visit_Try(self, node: ast.Try) -> None:
        self.has_try = True
        self._nested(node)

    def visit_With(self, node: ast.With) -> None:
        self._nested(node)

    def _bump_branch(self) -> None:
        if self._func_stack:
            top = self._func_stack[-1]
            self._func_stack[-1] = top.model_copy(update={"branch_count": top.branch_count + 1})

    # -- imports -----------------------------------------------------------
    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.imports.append(alias.name.split(".")[0])
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module:
            self.imports.append(node.module.split(".")[0])
        self.generic_visit(node)

    # -- assignments: df vars, collected vars, secrets, paths ---------------
    def visit_Assign(self, node: ast.Assign) -> None:
        targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        value = node.value
        if isinstance(value, ast.Call):
            root, attrs = _chain(value)
            outer = attrs[0] if attrs else ""
            for target in targets:
                self.assignments.append(
                    VarAssignment(target=target, source=root or "", line=node.lineno)
                )
            produces_df = (
                root == "spark"
                or root in self.df_vars
                or (isinstance(value.func, ast.Name) and value.func.id == "broadcast")
            ) and outer not in TERMINAL_ACTIONS
            if produces_df:
                for target in targets:
                    if target not in self.df_vars:
                        self.df_vars.append(target)
            if outer in ("collect", "toPandas", "to_pandas", "take", "first", "head"):
                for target in targets:
                    self.collected_vars[target] = outer
        for target_node, target in zip(node.targets, targets, strict=False):
            if SECRET_NAME_RE.search(target):
                self.secrets.append(
                    SecretRef(
                        line=getattr(target_node, "lineno", 0),
                        column=getattr(target_node, "col_offset", 0),
                        kind="credential-variable",
                    )
                )
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            for target in targets:
                self.string_vars[target] = value.value
        self._scan_strings(node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if (
            isinstance(node.target, ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            self.string_vars[node.target.id] = node.value.value
        self._scan_strings(node.value)
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr) -> None:
        self._scan_strings(node.value)
        self.generic_visit(node)

    def _scan_strings(self, node: ast.AST | None) -> None:
        for child in ast.walk(node) if node is not None else ():
            if isinstance(child, ast.Constant) and isinstance(child.value, str):
                if PATH_VALUE_RE.match(child.value):
                    self.paths.append(
                        PathRef(
                            line=getattr(child, "lineno", 0),
                            column=getattr(child, "col_offset", 0),
                            value=child.value[:120],
                        )
                    )

    def visit_Call(self, node: ast.Call) -> None:
        # keyword secrets: password="..." (value never stored)
        for kw in node.keywords:
            if kw.arg and SECRET_NAME_RE.search(kw.arg):
                self.secrets.append(
                    SecretRef(
                        line=getattr(node, "lineno", 0),
                        column=getattr(node, "col_offset", 0),
                        kind="credential-keyword",
                    )
                )
        self.generic_visit(node)

    def _check_driver_loop(self, node: ast.For) -> None:
        names: list[str] = []

        def _names(n: ast.AST) -> None:
            if isinstance(n, ast.Name):
                names.append(n.id)
            for child in ast.iter_child_nodes(n):
                _names(child)

        _names(node.iter)
        for name in names:
            if name in self.collected_vars:
                self.driver_loops.append(
                    {
                        "line": node.lineno,
                        "variable": name,
                        "collected_via": self.collected_vars[name],
                    }
                )


def _is_window_chain(attrs: list[str]) -> bool:
    return any(a in WINDOW_SPEC_ATTRS for a in attrs)


def _find_existence_calls(tree: ast.AST) -> set[ast.Call]:
    """Identify Call nodes that are evaluated solely to test non-emptiness/existence."""
    existence_calls: set[ast.Call] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            # e.g. df.count() > 0, df.count() == 0, df.count() != 0, df.count() >= 1
            if isinstance(node.left, ast.Call):
                for comp in node.comparators:
                    if isinstance(comp, ast.Constant) and comp.value in (0, 1):
                        existence_calls.add(node.left)
            # e.g. 0 < df.count(), 0 == df.count()
            for comp in node.comparators:
                if (
                    isinstance(comp, ast.Call)
                    and isinstance(node.left, ast.Constant)
                    and node.left.value in (0, 1)
                ):
                    existence_calls.add(comp)
        elif isinstance(node, (ast.If, ast.While)):
            if isinstance(node.test, ast.Call):
                existence_calls.add(node.test)
            elif (
                isinstance(node.test, ast.UnaryOp)
                and isinstance(node.test.op, ast.Not)
                and isinstance(node.test.operand, ast.Call)
            ):
                existence_calls.add(node.test.operand)
        elif (
            isinstance(node, ast.UnaryOp)
            and isinstance(node.op, ast.Not)
            and isinstance(node.operand, ast.Call)
        ):
            existence_calls.add(node.operand)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "bool"
            and node.args
            and isinstance(node.args[0], ast.Call)
        ):
            existence_calls.add(node.args[0])
    return existence_calls


def _record_operations(source: str, tree: ast.AST, df_vars: list[str]) -> list[Operation]:
    """Second pass: every Call node becomes a typed Operation (or skipped)."""
    ops: list[Operation] = []
    df_set = set(df_vars)
    existence_calls = _find_existence_calls(tree)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                dec_name = ""
                if isinstance(dec, ast.Name):
                    dec_name = dec.id
                elif isinstance(dec, ast.Attribute):
                    dec_name = dec.attr
                elif isinstance(dec, ast.Call):
                    f = dec.func
                    dec_name = f.id if isinstance(f, ast.Name) else getattr(f, "attr", "")
                if dec_name == "pandas_udf":
                    ops.append(
                        Operation(
                            operation_type=OperationType.PANDAS_UDF,
                            line=node.lineno,
                            column=node.col_offset,
                            code=f"@{dec_name} def {node.name}(...)",
                            arguments={"function": node.name},
                        )
                    )
                elif dec_name == "udf":
                    ops.append(
                        Operation(
                            operation_type=OperationType.UDF,
                            line=node.lineno,
                            column=node.col_offset,
                            code=f"@{dec_name} def {node.name}(...)",
                            arguments={"function": node.name},
                        )
                    )
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # bare function calls: udf(, pandas_udf(, broadcast(
        if isinstance(func, ast.Name):
            if func.id == "udf":
                ops.append(
                    Operation(
                        operation_type=OperationType.UDF,
                        line=node.lineno,
                        column=node.col_offset,
                        code=_snippet(source, node),
                        arguments={"kind": "python-udf"},
                    )
                )
            elif func.id == "pandas_udf":
                ops.append(
                    Operation(
                        operation_type=OperationType.PANDAS_UDF,
                        line=node.lineno,
                        column=node.col_offset,
                        code=_snippet(source, node),
                        arguments={"kind": "pandas-udf"},
                    )
                )
            elif func.id == "broadcast":
                arg0 = _snippet(source, node.args[0]) if node.args else ""
                ops.append(
                    Operation(
                        operation_type=OperationType.BROADCAST,
                        line=node.lineno,
                        column=node.col_offset,
                        dataframe=arg0 or None,
                        code=_snippet(source, node),
                        arguments={"dataframe": arg0},
                    )
                )
            continue
        if not isinstance(func, ast.Attribute):
            continue
        root, attrs = _chain(node)
        if not attrs:
            continue
        outer = attrs[0]
        snippet = _snippet(source, node)

        # spark.read... / spark.sql(...) / spark.table(...)
        # Only terminal calls (load/parquet/.../sql/table) record a READ so
        # intermediate chain links (format/option/mode) are not double-counted.
        if root == "spark":
            if outer in ("sql", "table"):
                ops.append(
                    Operation(
                        operation_type=OperationType.READ,
                        line=node.lineno,
                        column=node.col_offset,
                        code=snippet,
                        arguments={
                            "via": outer,
                            "query": _snippet(source, node.args[0])[:200] if node.args else "",
                        },
                    )
                )
                continue
            elif outer in ("load", "parquet", "csv", "json", "orc", "avro"):
                ops.append(
                    Operation(
                        operation_type=OperationType.READ,
                        line=node.lineno,
                        column=node.col_offset,
                        code=snippet,
                        arguments={"via": "read"},
                    )
                )
                continue
        # df.write... / df.writeStream... terminal writers
        # ("writeStream" chains terminate at .start(...) / .toTable(...);
        # streaming-ness is preserved in via, volume stays UNKNOWN.)
        is_stream_write = "writeStream" in attrs
        if "write" in attrs or is_stream_write:
            if "coalesce" in attrs:
                ops.append(
                    Operation(
                        operation_type=OperationType.COALESCE,
                        line=node.lineno,
                        column=node.col_offset,
                        dataframe=root,
                        code=snippet,
                        arguments={"literals": [1] if "coalesce(1)" in snippet else []},
                    )
                )
            if "repartition" in attrs:
                ops.append(
                    Operation(
                        operation_type=OperationType.REPARTITION,
                        line=node.lineno,
                        column=node.col_offset,
                        dataframe=root,
                        code=snippet,
                    )
                )
            if outer in SPARK_WRITE_ATTRS or outer in WRITE_CHAIN_ATTRS or (
                is_stream_write and outer in ("start", "toTable")
            ):
                # record once at the terminal save call
                if outer in ("save", "parquet", "csv", "json") or (
                    outer in SPARK_WRITE_ATTRS and outer not in ("write",)
                ) or (is_stream_write and outer in ("start", "toTable")):
                    ops.append(
                        Operation(
                            operation_type=OperationType.WRITE,
                            line=node.lineno,
                            column=node.col_offset,
                            dataframe=root,
                            code=snippet,
                            arguments={"via": outer},
                        )
                    )
            continue
        # Window spec chains / .over(...)
        if outer == "over" or (root == "Window" and _is_window_chain(attrs)):
            chain_args: dict[str, object] = {"chain": list(attrs)}
            if root == "Window":
                chain_args["bounded"] = any(a in ("rowsBetween", "rangeBetween") for a in attrs)
                chain_args["partitioned"] = "partitionBy" in attrs
            else:
                chain_args["bounded"] = "unknown"
                chain_args["partitioned"] = "unknown"
            ops.append(
                Operation(
                    operation_type=OperationType.WINDOW,
                    line=node.lineno,
                    column=node.col_offset,
                    dataframe=root if root != "Window" else None,
                    code=snippet,
                    arguments=chain_args,
                )
            )
            continue
        # dataframe method calls
        if root in df_set or root is None or root == "spark":
            op_type = METHOD_OPS.get(outer)
            if op_type is None:
                continue
            args: dict[str, object] = {}
            ctx: dict[str, object] = {}
            if op_type == OperationType.JOIN:
                other = _snippet(source, node.args[0])[:120] if node.args else ""
                how = ""
                on = ""
                for kw in node.keywords:
                    if kw.arg == "how":
                        c_how = _const_str(kw.value)
                        raw_how = _snippet(source, kw.value).strip("'\"")[:80]
                        how = c_how if c_how is not None else raw_how
                    elif kw.arg == "on":
                        on = _snippet(source, kw.value)[:80]
                if not how and len(node.args) > 1:
                    maybe = _const_str(node.args[1])
                    if maybe and maybe.lower() in _JOIN_ARG_TYPES:
                        how = maybe
                if not how and len(node.args) > 2:
                    maybe = _const_str(node.args[2])
                    if maybe and maybe.lower() in _JOIN_ARG_TYPES:
                        how = maybe
                default_how = "cross" if outer == "crossJoin" else "inner"
                args = {"other": other, "how": (how.strip("'\"") if how else "") or default_how}
                if on:
                    args["on"] = on
                if outer == "crossJoin":
                    args["how"] = "cross"
            elif op_type in (
                OperationType.REPARTITION,
                OperationType.COALESCE,
                OperationType.TAKE,
                OperationType.LIMIT,
            ):
                nums = [_const_number(a) for a in node.args]
                args = {
                    "arg_count": len(node.args),
                    "literals": [n for n in nums if n is not None],
                    "columns": [
                        _snippet(source, a)[:60] for a in node.args if _const_number(a) is None
                    ],
                }
            elif op_type == OperationType.SHOW:
                args = {"truncated": True}
            elif op_type in (OperationType.UDF,):
                args = {"kind": "python-udf"}
            elif op_type == OperationType.COUNT:
                if node in existence_calls:
                    args["existence_check"] = True
                    ctx["existence_check"] = True
            # limit/filter context for downstream driver-op analysis
            chain_set = set(attrs)
            if "limit" in chain_set:
                ctx["limited"] = True
                for a in node.args:
                    n = _const_number(a)
                    if n is not None:
                        ctx["limit_n"] = n
            if chain_set & {"filter", "where"}:
                ctx["filtered"] = True
            ops.append(
                Operation(
                    operation_type=op_type,
                    line=node.lineno,
                    column=node.col_offset,
                    dataframe=root,
                    code=snippet,
                    arguments=args,
                    context=ctx,
                )
            )
    ops.sort(key=lambda o: (o.line, o.column))
    return _dedupe_window_links(ops)


def _dedupe_window_links(ops: list[Operation]) -> list[Operation]:
    """Keep only the outermost op of each same-line Window spec chain.

    ``Window.partitionBy(..).orderBy(..).rowsBetween(..)`` visits three Call
    nodes; only the longest chain describes the full spec.
    """
    by_site: dict[tuple[int, str | None], list[Operation]] = {}
    for op in ops:
        if op.operation_type == OperationType.WINDOW:
            by_site.setdefault((op.line, op.dataframe), []).append(op)

    def _chain_len(op: Operation) -> int:
        chain = op.arguments.get("chain", [])
        return len(chain) if isinstance(chain, list) else 0

    drop: set[int] = set()
    for group in by_site.values():
        if len(group) > 1:
            best = max(group, key=_chain_len)
            drop.update(id(o) for o in group if o is not best)
    return [op for op in ops if id(op) not in drop]


def _resolve_sql_arg(node: ast.AST, string_vars: dict[str, str]) -> str | None:
    """Resolve an AST node representing a SQL argument to its string content."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in string_vars:
        return string_vars[node.id]
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for val in node.values:
            if isinstance(val, ast.Constant) and isinstance(val.value, str):
                parts.append(val.value)
            elif isinstance(val, ast.FormattedValue):
                parts.append("__param__")
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _resolve_sql_arg(node.left, string_vars)
        right = _resolve_sql_arg(node.right, string_vars)
        if left is not None and right is not None:
            return left + right
    return None


def _merge_sql_analyses(analyses: list[Any], filename: str) -> Any:
    """Combine multiple SQL analyses into one unified analysis."""
    from dpif.sql.models import SQLAnalysis, SQLDialect

    total_complexity: dict[str, int] = {}
    for a in analyses:
        for k, v in a.complexity.items():
            if isinstance(v, (int, float)):
                total_complexity[k] = total_complexity.get(k, 0) + int(v)

    return SQLAnalysis(
        source_file=filename,
        dialect=analyses[0].dialect if analyses else SQLDialect.DATABRICKS,
        query_count=sum(a.query_count for a in analyses),
        queries=[q for a in analyses for q in a.queries],
        tables=[t for a in analyses for t in a.tables],
        joins=[j for a in analyses for j in a.joins],
        filters=[f for a in analyses for f in a.filters],
        aggregations=[agg for a in analyses for agg in a.aggregations],
        windows=[w for a in analyses for w in a.windows],
        ctes=[c for a in analyses for c in a.ctes],
        subqueries=[s for a in analyses for s in a.subqueries],
        unions=[u for a in analyses for u in a.unions],
        wildcard_projections=[wp for a in analyses for wp in a.wildcard_projections],
        complexity=total_complexity,
        parse_errors=[err for a in analyses for err in a.parse_errors],
    )


def _extract_embedded_sql(
    tree: ast.AST,
    string_vars: dict[str, str],
    filename: str,
) -> Any | None:
    """Extract and parse SQL from spark.sql(...) calls in the AST."""
    from dpif.sql.parser import SQLParser

    analyses: list[Any] = []
    parser = SQLParser()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_spark_sql = False
        if isinstance(func, ast.Attribute) and func.attr == "sql":
            if isinstance(func.value, ast.Name) and func.value.id == "spark":
                is_spark_sql = True
            elif isinstance(func.value, ast.Call):
                root, _ = _chain(func.value)
                if root == "spark":
                    is_spark_sql = True
            elif isinstance(func.value, ast.Attribute):
                root, _ = _chain(node)
                if root == "spark":
                    is_spark_sql = True
            else:
                is_spark_sql = True

        if is_spark_sql and node.args:
            sql_text = _resolve_sql_arg(node.args[0], string_vars)
            if sql_text:
                call_loc = f"{filename}:{node.lineno}"
                parsed = parser.parse(sql_text, source_file=call_loc)
                analyses.append(parsed.analysis)

    if not analyses:
        return None
    if len(analyses) == 1:
        return analyses[0]
    return _merge_sql_analyses(analyses, filename)


def analyze_source(code: str, filename: str = "<code>") -> CodeAnalysis:
    """Parse Python or SQL source into a structured CodeAnalysis. Never raises."""
    if filename.endswith(".sql"):
        from dpif.sql.parser import SQLParser

        sql_parser = SQLParser()
        # Phase 10 (P10-2): provider TASK headers are authoritative task
        # metadata, not SQL. Parse a sanitized copy (headers -> blank lines,
        # line numbers preserved); the original blob stays authoritative for
        # task attribution.
        sql_res = sql_parser.parse(strip_task_headers_for_sql(code), source_file=filename)
        non_blank = [ln for ln in code.splitlines() if ln.strip()]
        return CodeAnalysis(
            source_file=filename,
            language="sql",
            lines_of_code=len(non_blank),
            sql_analysis=sql_res.analysis,
            parse_error=sql_res.parse_errors[0].message if sql_res.parse_errors else None,
        )

    try:
        tree = ast.parse(code, filename=filename)
    except SyntaxError as e:
        from dpif.sql.parser import SQLParser

        sql_parser = SQLParser()
        # Phase 10 (P10-2): same header sanitation as the .sql branch above.
        sql_res = sql_parser.parse(strip_task_headers_for_sql(code), source_file=filename)
        if sql_res.analysis.queries and not sql_res.parse_errors:
            non_blank = [ln for ln in code.splitlines() if ln.strip()]
            return CodeAnalysis(
                source_file=filename,
                language="sql",
                lines_of_code=len(non_blank),
                sql_analysis=sql_res.analysis,
            )
        return CodeAnalysis(
            source_file=filename,
            parse_error=f"{e.msg} at line {e.lineno}",
            lines_of_code=len(code.splitlines()),
        )

    visitor = _StructureVisitor(code)
    visitor.visit(tree)
    operations = _record_operations(code, tree, visitor.df_vars)
    assignments = visitor.assignments
    non_blank = [ln for ln in code.splitlines() if ln.strip()]
    embedded_sql = _extract_embedded_sql(tree, visitor.string_vars, filename)

    return CodeAnalysis(
        source_file=filename,
        language="python",
        functions=visitor.functions,
        imports=sorted(set(visitor.imports)),
        dataframe_variables=visitor.df_vars,
        assignments=assignments,
        operations=operations,
        driver_loops=visitor.driver_loops,
        secrets=visitor.secrets,
        hardcoded_paths=visitor.paths,
        lines_of_code=len(non_blank),
        has_exception_handling=visitor.has_try,
        max_nesting_depth=visitor.max_nesting,
        sql_analysis=embedded_sql,
    )


# -- Phase 9 (P9-2): task attribution tagging --------------------------------
# Attribution uses ONLY authoritative task boundaries:
#   1. provider-generated task code reference (per-task entry path/filename),
#   2. exact source-file boundary (whole single-file analysis),
#   3. explicit provider task headers of the exact form emitted below.
# Forbidden: line/order/timing/AST/variable proximity, ordering, "closest
# task", or any other heuristic. No authoritative boundary -> task_key None.

TASK_HEADER_RE = re.compile(r"^# --- TASK: (.+?) \(.*\) ---\s*$")


def strip_task_headers_for_sql(code: str) -> str:
    """Replace provider TASK header lines with blank lines (Phase 10, P10-2).

    Used ONLY on a sanitized copy handed to ``SQLParser``: ``#`` is not a SQL
    comment, so an authoritative provider header would otherwise fail SQL
    parsing and hide genuine SQL. Only lines matching the strict
    ``TASK_HEADER_RE`` are blanked; everything else — including ``--`` /
    ``/* */`` comments, ``#`` inside string literals, and all other content
    — passes through byte-identical.

    Newline representation is preserved per line (LF stays LF, CRLF stays
    CRLF): matching is performed against the line body with its original
    terminator re-attached, so total line count and all non-header bytes are
    unchanged. The original text is never mutated by this helper; callers
    must keep parsing the original for task attribution.
    """
    if not code:
        return code
    parts: list[str] = []
    for line in code.splitlines(keepends=True):
        if line.endswith("\r\n"):
            body, terminator = line[:-2], "\r\n"
        elif line.endswith("\n") or line.endswith("\r"):
            body, terminator = line[:-1], line[-1:]
        else:
            body, terminator = line, ""
        parts.append(terminator if TASK_HEADER_RE.match(body) else line)
    return "".join(parts)


def parse_task_boundaries(combined_code: str) -> list[tuple[str, int]]:
    """Map provider task headers to 1-based start lines, in file order.

    Returns ``[(task_key, start_line), ...]``. A task owns every line from
    its header until the next header (or end of file); lines before the
    first header belong to no task. Malformed headers are ignored (no
    boundary is better than a guessed one).
    """
    boundaries: list[tuple[str, int]] = []
    for lineno, line in enumerate((combined_code or "").splitlines(), start=1):
        match = TASK_HEADER_RE.match(line)
        if match:
            task_key = match.group(1).strip()
            if task_key:
                boundaries.append((task_key, lineno))
    return boundaries


def tag_operations_with_tasks(
    analysis: CodeAnalysis,
    boundaries: list[tuple[str, int]] | None,
) -> int:
    """Assign ``task_key`` on operations covered by exactly one boundary range.

    Positional containment in a provider-generated boundary is the only
    signal used. Operations outside every range, or in overlapping ranges
    (which providers must never emit), keep ``task_key=None`` (UNKNOWN).
    Returns the number of operations tagged. The analysis object is mutated
    in place; callers needing the untagged original must pass a copy.
    """
    if analysis is None or not boundaries:
        return 0
    tagged = 0
    spans = [(key, start) for key, start in boundaries]
    for op in analysis.operations:
        owner: str | None = None
        ambiguous = False
        for index, (key, start) in enumerate(spans):
            end = spans[index + 1][1] if index + 1 < len(spans) else None
            if op.line >= start and (end is None or op.line < end):
                if owner is None:
                    owner = key
                else:
                    ambiguous = True
                    break
        if owner is not None and not ambiguous:
            op.task_key = owner
            tagged += 1
    return tagged


def analyze_tasks_for_attribution(
    discovered_tasks: list[dict[str, Any]] | None,
) -> dict[str, CodeAnalysis | None]:
    """Run per-task ``analyze_source`` for attribution metadata ONLY.

    Returns ``{task_key: CodeAnalysis | None}`` for entries that carry
    retrievable code (ANALYZED entries with ``source_code``); retrieval
    failures, unsupported entries (no code), and parse errors yield
    ``None`` — never fabricated operations. Results must never feed
    checkpoints, rules, M5E/M5G, or any decision input.
    """
    results: dict[str, CodeAnalysis | None] = {}
    for task in discovered_tasks or []:
        if not isinstance(task, dict):
            continue
        task_key = str(task.get("task_key", "task"))
        code = task.get("source_code")
        if not isinstance(code, str) or not code:
            results[task_key] = None
            continue
        language = str(task.get("language", "python"))
        filename = f"{task_key}.sql" if language == "sql" else f"{task_key}.py"
        try:
            results[task_key] = analyze_source(code, filename=filename)
        except Exception:
            results[task_key] = None
    return results
