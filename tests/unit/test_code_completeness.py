"""Unit tests for Phase 4 code completeness intelligence.

Covers (per approved scope A-Y):
- Lexical markers: TODO / FIXME / HACK / case handling / docstring exclusion
- Stubs: pass / ellipsis bodies + abstract/overload/handler allowlists
- NotImplementedError incl. attribute form + placeholder returns
- Parse failure -> UNKNOWN (never PASS)
- Structural consumer: dead-end ops, unused datasets, disconnected joins,
  unreachable targets, reachability verdicts, terminal protection
- SQL unused CTE (+ parse-failure SQL -> UNKNOWN, recursive-CTE protection)
- Function-scope false-positive protection
- Offline / online / parity via the shared M5E + builder path
- Phase 1/2/3 behavior intact (no per-op volume, no invented correlation)
"""

from __future__ import annotations

from dpif.analyzers.implementation import DeveloperImplementationAnalyzer
from dpif.code.parser import analyze_source
from dpif.flow import build_pipeline_flow_graph
from dpif.flow.completeness import (
    CompletenessSignalKind,
    TargetReachability,
    analyze_structural_completeness,
    target_reachability,
)
from dpif.models import CheckpointStatus
from dpif.sql.parser import SQLParser


def _analyze(code: str, filename: str = "p.py", **ctx):
    analysis = analyze_source(code, filename=filename)
    context = {"code_snippet": code, "evidence_source": "static code"}
    context.update(ctx)
    return DeveloperImplementationAnalyzer(analysis, context=context)


def _comp_rules(result) -> set[str]:
    return {f.rule_id for f in result.all_findings if f.rule_id.startswith("IMP-COMP")}


def _graph(code: str, filename: str = "p.py", **kwargs):
    analysis = analyze_source(code, filename=filename)
    return build_pipeline_flow_graph(
        code_analysis=analysis, pipeline_name="t", raw_code=code, **kwargs
    )


# ==============================================================================
# A/B/C/D. Markers: TODO / FIXME / HACK / case handling
# ==============================================================================


def test_todo_marker_detected():
    r = _analyze("df = spark.read.table('a')\n# TODO: handle nulls\ndf.write.save('o')\n").analyze()
    assert "IMP-COMP-002" in _comp_rules(r)


def test_fixme_marker_detected():
    r = _analyze("x = 1  # FIXME: magic number\n").analyze()
    assert "IMP-COMP-002" in _comp_rules(r)


def test_hack_marker_detected():
    r = _analyze("x = 1  # HACK: workaround for now\n").analyze()
    assert "IMP-COMP-002" in _comp_rules(r)


def test_lowercase_markers_detected():
    r = _analyze("x = 1  # todo: lowercase still counts\n").analyze()
    assert "IMP-COMP-002" in _comp_rules(r)


def test_lowercase_xxx_not_flagged():
    """Lowercase xxx collides with placeholder content — must stay silent."""
    r = _analyze("x = 'xxx'\n").analyze()
    assert "IMP-COMP-002" not in _comp_rules(r)


def test_docstring_todo_not_a_defect():
    code = '"""Module docs.\n\nTODO: this documents future work, not code debt.\n"""\nx = 1\n'
    r = _analyze(code).analyze()
    assert "IMP-COMP-002" not in _comp_rules(r)


def test_clean_code_has_no_marker_finding():
    r = _analyze("df = spark.read.table('a')\ndf.write.save('o')\n").analyze()
    assert "IMP-COMP-002" not in _comp_rules(r)


# ==============================================================================
# E/F. Pass stubs + allowlists
# ==============================================================================


def test_pass_stub_detected():
    r = _analyze("def helper():\n    pass\n").analyze()
    assert "IMP-COMP-003" in _comp_rules(r)


def test_ellipsis_stub_detected():
    r = _analyze("def helper():\n    ...\n").analyze()
    assert "IMP-COMP-003" in _comp_rules(r)


def test_abstract_method_pass_allowed():
    code = (
        "import abc\n"
        "class Base(abc.ABC):\n"
        "    @abc.abstractmethod\n"
        "    def run(self):\n"
        "        pass\n"
    )
    r = _analyze(code).analyze()
    assert "IMP-COMP-003" not in _comp_rules(r)


def test_except_handler_pass_allowed():
    code = "try:\n    risky()\nexcept ValueError:\n    pass\n"
    r = _analyze(code).analyze()
    assert "IMP-COMP-003" not in _comp_rules(r)


def test_empty_class_pass_allowed():
    r = _analyze("class Config:\n    pass\n").analyze()
    assert "IMP-COMP-003" not in _comp_rules(r)


# ==============================================================================
# G/H. NotImplementedError forms
# ==============================================================================


def test_bare_not_implemented_error_detected():
    r = _analyze("def f():\n    raise NotImplementedError\n").analyze()
    assert "IMP-COMP-001" in _comp_rules(r)


def test_call_not_implemented_error_detected():
    r = _analyze("def f():\n    raise NotImplementedError('todo')\n").analyze()
    assert "IMP-COMP-001" in _comp_rules(r)


def test_attribute_form_not_implemented_error_detected():
    r = _analyze("import builtins\ndef f():\n    raise builtins.NotImplementedError\n").analyze()
    assert "IMP-COMP-001" in _comp_rules(r)


def test_not_implemented_singleton_detected():
    r = _analyze("def f():\n    raise NotImplemented\n").analyze()
    assert "IMP-COMP-001" in _comp_rules(r)


# ==============================================================================
# I. Placeholder returns
# ==============================================================================


def test_placeholder_return_none_detected():
    r = _analyze("def noop():\n    return None\n").analyze()
    assert "IMP-COMP-004" in _comp_rules(r)


def test_real_function_body_not_flagged():
    r = _analyze("def f():\n    x = 1\n    return x\n").analyze()
    assert "IMP-COMP-003" not in _comp_rules(r)
    assert "IMP-COMP-004" not in _comp_rules(r)


# ==============================================================================
# J. Parse failure -> UNKNOWN, never PASS
# ==============================================================================


def test_parse_failure_yields_unknown_not_pass():
    analysis = analyze_source("def broken(:\n  pass\n", filename="b.py")
    assert analysis.parse_error
    result = DeveloperImplementationAnalyzer(analysis, context={}).analyze()
    comp = [f for f in result.all_findings if f.rule_id == "IMP-COMP-000"]
    assert len(comp) == 1
    assert comp[0].status == CheckpointStatus.UNKNOWN


# ==============================================================================
# K/L/M/N/O. Structural consumer via graph + analyzer
# ==============================================================================


def test_dead_end_operation_signal():
    g = _graph("df = spark.read.table('a.b')\ndf.filter('x > 1')\n")
    res = analyze_structural_completeness(g)
    kinds = {s.kind for s in res.signals}
    assert CompletenessSignalKind.DEAD_END_OPERATION in kinds


def test_analyzer_reports_dead_end_operation():
    code = "df = spark.read.table('a.b')\ndf.filter('x > 1')\n"
    analysis = analyze_source(code, filename="p.py")
    g = build_pipeline_flow_graph(code_analysis=analysis, pipeline_name="t", raw_code=code)
    result = DeveloperImplementationAnalyzer(
        analysis, context={"code_snippet": code, "flow_graph": g}
    ).analyze()
    assert "IMP-COMP-010" in _comp_rules(result)


def test_unreachable_target_signal_and_finding():
    code = (
        "a = spark.read.table('s.t')\n"
        "c = other.filter('x > 1')\n"
        "c.write.saveAsTable('o.t')\n"
    )
    analysis = analyze_source(code, filename="p.py")
    g = build_pipeline_flow_graph(code_analysis=analysis, pipeline_name="t", raw_code=code)
    res = analyze_structural_completeness(g)
    assert res.verdict == TargetReachability.TARGET_NOT_REACHABLE
    assert CompletenessSignalKind.UNREACHABLE_TARGET in {s.kind for s in res.signals}
    result = DeveloperImplementationAnalyzer(
        analysis, context={"code_snippet": code, "flow_graph": g}
    ).analyze()
    assert "IMP-COMP-013" in _comp_rules(result)


def test_reachable_target_no_false_positive():
    code = "df = spark.read.table('a.b')\ndf.write.saveAsTable('c.d')\n"
    g = _graph(code)
    res = analyze_structural_completeness(g)
    assert res.verdict == TargetReachability.TARGET_REACHABLE
    assert res.signals == []


def test_no_targets_verdict_unknown():
    g = _graph("df = spark.read.table('a.b')\n")
    assert target_reachability(g) == TargetReachability.UNKNOWN
    assert target_reachability(None) == TargetReachability.UNKNOWN


def test_disconnected_branch_dataset_signal():
    code = (
        "df = spark.read.table('a.b')\n"
        "dead = df.filter('x > 1')\n"
        "live = df.filter('y < 2')\n"
        "live.write.saveAsTable('c.d')\n"
    )
    g = _graph(code)
    kinds = {s.kind for s in analyze_structural_completeness(g).signals}
    assert CompletenessSignalKind.UNUSED_DATASET in kinds


def test_disconnected_join_signal():
    code = (
        "df = spark.read.table('a.b')\n"
        "j = df.join(missing_df, 'id')\n"
        "j.write.saveAsTable('c.d')\n"
    )
    g = _graph(code)
    kinds = {s.kind for s in analyze_structural_completeness(g).signals}
    assert CompletenessSignalKind.DISCONNECTED_JOIN in kinds


# ==============================================================================
# P/Q. SQL unused CTE
# ==============================================================================


def test_unused_cte_referenced_by_populated():
    parser = SQLParser()
    result = parser.parse(
        "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a",
        source_file="t.sql",
    )
    by_name = {c.name: c.referenced_by for c in result.analysis.ctes}
    assert by_name["a"] == ["__main__"]
    assert by_name["b"] == []


def test_unused_cte_finding():
    code = "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a\n"
    analysis = analyze_source(code, filename="t.sql")
    result = DeveloperImplementationAnalyzer(
        analysis, context={"code_snippet": code}
    ).analyze()
    cte_findings = [f for f in result.all_findings if f.rule_id == "IMP-COMP-020"]
    assert len(cte_findings) == 1
    assert cte_findings[0].observed["cte"] == "b"


def test_fully_used_ctes_no_finding():
    code = "WITH a AS (SELECT 1 AS x) SELECT * FROM a\n"
    analysis = analyze_source(code, filename="t.sql")
    result = DeveloperImplementationAnalyzer(
        analysis, context={"code_snippet": code}
    ).analyze()
    assert "IMP-COMP-020" not in _comp_rules(result)


def test_recursive_cte_not_flagged():
    code = (
        "WITH RECURSIVE fib(n, f) AS ("
        "SELECT 1, 1 UNION ALL SELECT n + 1, f + n FROM fib WHERE n < 10"
        ") SELECT * FROM fib\n"
    )
    analysis = analyze_source(code, filename="t.sql")
    result = DeveloperImplementationAnalyzer(
        analysis, context={"code_snippet": code}
    ).analyze()
    assert "IMP-COMP-020" not in _comp_rules(result)


def test_malformed_sql_no_cte_finding():
    analysis = analyze_source("WITH a AS (SELECT FROM WHERE (((", filename="t.sql")
    result = DeveloperImplementationAnalyzer(
        analysis, context={"code_snippet": "WITH a AS (SELECT FROM WHERE ((("}
    ).analyze()
    rules = _comp_rules(result)
    assert "IMP-COMP-020" not in rules


# ==============================================================================
# R/S. False-positive protection
# ==============================================================================


def test_function_returning_dataframe_not_a_stub():
    code = (
        "def get_df():\n"
        "    df = spark.read.table('a.b')\n"
        "    return df\n"
        "out = get_df()\n"
        "out.write.saveAsTable('c.d')\n"
    )
    r = _analyze(code).analyze()
    rules = _comp_rules(r)
    assert "IMP-COMP-003" not in rules
    assert "IMP-COMP-004" not in rules


def test_legitimate_terminals_protected():
    code = (
        "df = spark.read.table('a.b')\n"
        "df.cache()\n"
        "df.show()\n"
        "df.write.saveAsTable('c.d')\n"
    )
    g = _graph(code)
    kinds = {s.kind for s in analyze_structural_completeness(g).signals}
    assert CompletenessSignalKind.DEAD_END_OPERATION not in kinds


def test_valid_target_nodes_protected():
    code = (
        "df = spark.read.table('a.b')\n"
        "df.write.saveAsTable('c.d')\n"
        "df2 = spark.read.table('e.f')\n"
        "df2.write.saveAsTable('g.h')\n"
    )
    g = _graph(code)
    res = analyze_structural_completeness(g)
    assert res.verdict == TargetReachability.TARGET_REACHABLE
    assert res.signals == []


# ==============================================================================
# T/U/V. Offline / online / parity through the shared path
# ==============================================================================


def _m5e_rules(code: str, evidence_source: str, graph=None):
    analysis = analyze_source(code, filename="p.py")
    context = {"code_snippet": code, "evidence_source": evidence_source}
    if graph is not None:
        context["flow_graph"] = graph
    return _comp_rules(
        DeveloperImplementationAnalyzer(analysis, context=context).analyze()
    )


def test_offline_behavior_structural_and_lexical():
    code = "df = spark.read.table('a.b')\n# TODO: dedup\ndf.filter('x > 1')\n"
    analysis = analyze_source(code, filename="p.py")
    g = build_pipeline_flow_graph(code_analysis=analysis, pipeline_name="t", raw_code=code)
    rules = _m5e_rules(code, "fixture metadata", graph=g)
    assert "IMP-COMP-002" in rules
    assert "IMP-COMP-010" in rules


def test_online_behavior_same_shared_path():
    code = "df = spark.read.table('a.b')\n# TODO: dedup\ndf.filter('x > 1')\n"
    analysis = analyze_source(code, filename="task_key.py")
    g = build_pipeline_flow_graph(code_analysis=analysis, pipeline_name="t", raw_code=code)
    rules = _m5e_rules(code, "live databricks", graph=g)
    assert "IMP-COMP-002" in rules
    assert "IMP-COMP-010" in rules


def test_offline_online_parity():
    code = "df = spark.read.table('a.b')\n# TODO: dedup\ndf.filter('x > 1')\n"
    analysis = analyze_source(code, filename="p.py")
    g = build_pipeline_flow_graph(code_analysis=analysis, pipeline_name="t", raw_code=code)
    assert _m5e_rules(code, "fixture metadata", graph=g) == _m5e_rules(
        code, "live databricks", graph=g
    )


# ==============================================================================
# Phase 1/2/3 intact: no volume attribution, no invented correlation
# ==============================================================================


def test_completeness_adds_no_volume():
    code = "df = spark.read.table('a.b')\ndf.filter('x > 1')\n"
    g = _graph(code)
    res = analyze_structural_completeness(g)
    assert res.signals  # dead-end found...
    assert all(n.volume is None for n in g.nodes)  # ...but no bytes invented


def test_graph_serialization_unchanged_shape():
    code = "df = spark.read.table('a.b')\ndf.write.saveAsTable('c.d')\n"
    payload = _graph(code).to_dict()
    assert set(payload) >= {"pipeline_name", "nodes", "edges", "source_ids", "target_ids"}
