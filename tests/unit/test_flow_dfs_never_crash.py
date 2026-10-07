"""P10-1 regression tests: flow DFS never crashes on cyclic graphs (D1).

Defect: ``PipelineFlowGraph.structural_issues()`` treated every globally
GRAY node as a cycle. A genuine cycle made ``dfs()`` return early without
unwinding global GRAY state, so a later DFS root reaching a stale GRAY node
raised ``ValueError`` in ``path.index`` instead of reporting CYCLE.

Fix: cycle membership is current-recursion-path membership only
(``nxt in on_path``); global GRAY/BLACK remain traversal optimization.
"""

from __future__ import annotations

from pathlib import Path

from dpif.code.parser import analyze_source
from dpif.flow.builder import build_pipeline_flow_graph
from dpif.flow.models import FlowEdge, FlowNode, FlowNodeKind, PipelineFlowGraph


def _bad_pipeline_graph() -> PipelineFlowGraph:
    """Real failure pattern: variable rebinding across statements."""
    code = Path("tests/fixtures/code/bad_pipeline.py").read_text(encoding="utf-8")
    analysis = analyze_source(code, filename="bad_pipeline.py")
    return build_pipeline_flow_graph(
        code_analysis=analysis,
        contract=None,
        pipeline_name="bad",
        raw_code=code,
    )


def _graph(nodes, edges) -> PipelineFlowGraph:
    return PipelineFlowGraph(
        pipeline_name="p",
        nodes=[FlowNode(node_id=n, kind=FlowNodeKind.OPERATION) for n in nodes],
        edges=[FlowEdge(from_node=a, to_node=b) for a, b in edges],
    )


# A. structural_issues() does not raise on the real failure pattern.
def test_bad_pipeline_graph_never_raises():
    graph = _bad_pipeline_graph()
    issues = graph.structural_issues()
    assert isinstance(issues, list)


# B. The genuine cycle is reported.
def test_bad_pipeline_cycle_reported():
    graph = _bad_pipeline_graph()
    codes = {i.code for i in graph.structural_issues()}
    assert "CYCLE" in codes


# C. DFS continues after identifying the cycle (full issue list returned).
def test_continues_after_cycle():
    graph = _bad_pipeline_graph()
    first = graph.structural_issues()
    second = graph.structural_issues()
    assert [i.code for i in first] == [i.code for i in second]
    assert any(i.code == "CYCLE" for i in first)


# D. A later DFS root reaching stale-GRAY state does not raise and is not
# falsely classified as cyclic: cycle (m1<->m2) sorts before root z->m1.
def test_later_root_over_stale_state():
    graph = _graph(
        ["m1", "m2", "z"],
        [("m1", "m2"), ("m2", "m1"), ("z", "m1")],
    )
    issues = graph.structural_issues()
    codes = [i.code for i in issues]
    assert "CYCLE" in codes
    # z itself is not cyclic: no CYCLE issue may point at z as cycle start
    # while missing the genuine m-cycle.
    assert any(i.node_id in ("m1", "m2") for i in issues if i.code == "CYCLE")


# E. Acyclic graphs remain unchanged (no issues from the cycle detector).
def test_acyclic_graph_unchanged():
    graph = _graph(["a", "b", "c"], [("a", "b"), ("b", "c")])
    graph.source_ids = ["a"]
    graph.target_ids = ["c"]
    cycles = [i for i in graph.structural_issues() if i.code == "CYCLE"]
    assert cycles == []


# F. Multiple independent DFS roots each report their own genuine cycle.
def test_multiple_independent_cycles():
    graph = _graph(
        ["a1", "a2", "b1", "b2"],
        [("a1", "a2"), ("a2", "a1"), ("b1", "b2"), ("b2", "b1")],
    )
    cycles = [i for i in graph.structural_issues() if i.code == "CYCLE"]
    assert len(cycles) == 2
    starts = {i.node_id for i in cycles}
    assert starts & {"a1", "a2"}
    assert starts & {"b1", "b2"}


# Completed (BLACK) nodes are never treated as part of the current cycle.
def test_completed_node_not_on_path():
    graph = _graph(
        ["a", "b", "c", "d"],
        [("a", "b"), ("b", "c"), ("a", "c"), ("c", "d")],
    )
    graph.source_ids = ["a"]
    graph.target_ids = ["d"]
    cycles = [i for i in graph.structural_issues() if i.code == "CYCLE"]
    assert cycles == []
