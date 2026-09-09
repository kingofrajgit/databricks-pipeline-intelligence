"""Lightweight DataFrame flow representation (Phase 4, §7).

Not a Spark compiler: per-variable operation chains for contextual analysis.
"""

from __future__ import annotations

from dpif.code.models import CodeAnalysis, OperationType

ACTION_TYPES = frozenset(
    {
        OperationType.COLLECT,
        OperationType.TO_PANDAS,
        OperationType.SHOW,
        OperationType.TAKE,
        OperationType.FIRST,
        OperationType.COUNT,
        OperationType.WRITE,
    }
)


def build_flow(analysis: CodeAnalysis) -> dict[str, list[str]]:
    """Map each dataframe variable to its ordered operation-type chain."""
    flow: dict[str, list[str]] = {}
    for op in analysis.operations:
        if op.dataframe:
            flow.setdefault(op.dataframe, []).append(op.operation_type.value)
    return flow


def actions_on(analysis: CodeAnalysis, dataframe: str) -> list[str]:
    """Action operations recorded on one dataframe, in order."""
    return [
        op.operation_type.value
        for op in analysis.operations_on(dataframe)
        if op.operation_type in ACTION_TYPES
    ]


def downstream_uses(analysis: CodeAnalysis, dataframe: str, after_line: int) -> int:
    """Operations referencing a dataframe strictly after a line.

    Strictly-after so a cache()/persist() call never counts itself as reuse.
    """
    return sum(1 for op in analysis.operations_on(dataframe) if op.line > after_line)


def flow_root(analysis: CodeAnalysis, variable: str, before_line: int | None = None) -> str:
    """Resolve a variable to its ultimate source through assignments.

    ``huge = df.repartition(..)`` followed by ``huge.repartition(..)``
    groups under ``df`` so repeated shuffles across renames are visible.
    Self-edges (``huge = huge.op(..)``) resolve to the previous assignment;
    ``before_line`` bounds the walk so an operation sees only its inputs.
    """
    seen = {variable}
    current = variable
    bound = before_line
    while True:
        prev = None
        for edge in analysis.assignments:
            if edge.target != current or not edge.source or edge.source in seen:
                continue
            if bound is not None and edge.line >= bound:
                continue
            if prev is None or edge.line > prev.line:
                prev = edge
        if prev is None:
            return current
        seen.add(current)
        current = prev.source
        bound = prev.line
    return current
