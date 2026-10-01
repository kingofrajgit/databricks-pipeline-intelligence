"""Structural completeness consumer over PipelineFlowGraph (Phase 4).

Pure functions shared by BOTH validation paths: offline and online feed the
same common graph, so the same dead-end / disconnect / reachability logic
applies. No new graph model — this module only *reads* the Phase 1 graph
(``structural_issues()``, ``reaches_target()``, edges) and converts genuine
structural problems into completeness evidence.

Legitimate terminals are never defects: TARGET nodes, action sinks
(``collect/show/take/...``), cache/persist/checkpoint side effects, and
WRITE operations (which always carry their TARGET edge).

If the graph cannot establish a defect, the verdict is UNKNOWN — failures
are never manufactured.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

from dpif.code.flow import ACTION_TYPES
from dpif.code.models import OperationType
from dpif.flow.models import FlowNodeKind, PipelineFlowGraph


class TargetReachability(StrEnum):
    """Structural verdict: does the implementation reach a target?"""

    TARGET_REACHABLE = "TARGET_REACHABLE"
    TARGET_NOT_REACHABLE = "TARGET_NOT_REACHABLE"
    UNKNOWN = "UNKNOWN"


class CompletenessSignalKind(StrEnum):
    """Kind of structural completeness signal (all DERIVED heuristics)."""

    DEAD_END_OPERATION = "DEAD_END_OPERATION"
    UNUSED_DATASET = "UNUSED_DATASET"
    DISCONNECTED_JOIN = "DISCONNECTED_JOIN"
    UNREACHABLE_TARGET = "UNREACHABLE_TARGET"


class CompletenessSignal(BaseModel):
    """One structural completeness observation keyed by graph node."""

    node_id: str
    kind: CompletenessSignalKind
    reason: str
    reaches_target: bool | None = None


class StructuralCompleteness(BaseModel):
    """Verdict + signals for one flow graph."""

    verdict: TargetReachability = TargetReachability.UNKNOWN
    signals: list[CompletenessSignal] = Field(default_factory=list)


# Operation types that are legitimate terminals even with no outgoing edge:
# action sinks produce driver-side results, cache/persist/checkpoint are
# side effects, WRITE carries its TARGET edge by construction.
LEGITIMATE_TERMINALS = frozenset(
    set(ACTION_TYPES)
    | {
        OperationType.CACHE,
        OperationType.PERSIST,
        OperationType.CHECKPOINT,
    }
)


def target_reachability(graph: PipelineFlowGraph | None) -> TargetReachability:
    """Explicit structural verdict for ``source → operations → target``.

    TARGET_REACHABLE: every declared target is reachable from a source.
    TARGET_NOT_REACHABLE: at least one declared target is unreachable.
    UNKNOWN: no sources or no targets to judge (never invented).
    """
    if graph is None or not graph.source_ids or not graph.target_ids:
        return TargetReachability.UNKNOWN
    for tid in graph.target_ids:
        if graph.node(tid) is None:
            return TargetReachability.UNKNOWN
        reachable = any(
            tid in graph.descendants_of(sid)
            for sid in graph.source_ids
            if graph.node(sid) is not None
        )
        if not reachable:
            return TargetReachability.TARGET_NOT_REACHABLE
    return TargetReachability.TARGET_REACHABLE


def analyze_structural_completeness(
    graph: PipelineFlowGraph | None,
) -> StructuralCompleteness:
    """Convert genuine structural problems into completeness evidence.

    Returns UNKNOWN with no signals when the graph cannot establish a
    defect (missing graph, no sources, no targets).
    """
    if graph is None:
        return StructuralCompleteness(verdict=TargetReachability.UNKNOWN)
    verdict = target_reachability(graph)
    signals: list[CompletenessSignal] = []

    if verdict == TargetReachability.UNKNOWN and not graph.source_ids:
        return StructuralCompleteness(verdict=verdict, signals=signals)

    outgoing: dict[str, int] = {}
    incoming: dict[str, int] = {}
    for edge in graph.edges:
        outgoing[edge.from_node] = outgoing.get(edge.from_node, 0) + 1
        incoming[edge.to_node] = incoming.get(edge.to_node, 0) + 1

    for node in graph.nodes:
        out_count = outgoing.get(node.node_id, 0)
        if node.kind == FlowNodeKind.OPERATION and out_count == 0:
            if node.operation_type in LEGITIMATE_TERMINALS:
                continue
            signals.append(
                CompletenessSignal(
                    node_id=node.node_id,
                    kind=CompletenessSignalKind.DEAD_END_OPERATION,
                    reason=(
                        f"Operation '{node.label}' has no downstream edge; "
                        "its result never reaches a target or action"
                    ),
                    reaches_target=False,
                )
            )
        elif node.kind == FlowNodeKind.DATASET and out_count == 0:
            signals.append(
                CompletenessSignal(
                    node_id=node.node_id,
                    kind=CompletenessSignalKind.UNUSED_DATASET,
                    reason=(
                        f"Dataset '{node.name}' is assigned but never consumed "
                        "downstream in the analyzed scope"
                    ),
                    reaches_target=False,
                )
            )
        elif node.kind == FlowNodeKind.OPERATION and node.operation_type == OperationType.JOIN:
            if incoming.get(node.node_id, 0) < 2:
                signals.append(
                    CompletenessSignal(
                        node_id=node.node_id,
                        kind=CompletenessSignalKind.DISCONNECTED_JOIN,
                        reason=(
                            "Join has fewer than two resolved inputs; "
                            "the second input could not be connected"
                        ),
                        reaches_target=graph.reaches_target(node.node_id),
                    )
                )

    for issue in graph.structural_issues():
        if issue.code == "UNREACHABLE_TARGET" and issue.node_id:
            signals.append(
                CompletenessSignal(
                    node_id=issue.node_id,
                    kind=CompletenessSignalKind.UNREACHABLE_TARGET,
                    reason=issue.message,
                    reaches_target=False,
                )
            )

    # Deterministic order for stable reports/tests.
    signals.sort(key=lambda s: (s.kind.value, s.node_id))
    return StructuralCompleteness(verdict=verdict, signals=signals)


def to_dict(result: StructuralCompleteness) -> dict[str, object]:
    """JSON-safe serialization for reports."""
    return result.model_dump(mode="json")
